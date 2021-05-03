#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 syn=python

import argparse
import logging
import os
import re
import sqlite3
import urlparse
import warnings

from multiprocessing.pool import ThreadPool
from threading import Lock

from pyPodcastParser.Podcast import Podcast
from configparser import SafeConfigParser
import arrow
import requests

CKPOD_CONFIG = None
DB_LOCK = Lock()
program_args = None


def parse_args():
    global program_args

    descr = "Another (hopefully less terrible) podcast downloader"
    parser = argparse.ArgumentParser(
        description=descr, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c",
        "--confdir",
        dest="confdir",
        default=os.path.expanduser("~/.ckpod"),
        type=str,
        help="path of the configuration directory",
    )
    parser.add_argument(
        "-d",
        "--downloads",
        dest="downloads",
        default=4,
        type=int,
        help="number of simultaneous downloads",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        default=5,
        type=float,
        help="http idle timeout",
    )
    parser.add_argument(
        "-p",
        "--probe",
        dest="probe",
        type=str,
        help="probe the download links for the final url",
    )
    parser.add_argument(
        "-s",
        "--sed",
        dest="sed",
        type=str,
        help="test a sed pattern to convert a download url to a local disk filename",
    )
    parser.add_argument(
        "-r",
        "--refresh",
        dest="refresh",
        default=False,
        action="store_true",
        help="refresh episode list only",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        default=0,
        action="count",
        help="increase verbosity",
    )
    program_args = parser.parse_args()


def dbconnect(path=None):
    """
    Attach to the SQLite file, and set some useful properties

    Arguments: disk path of database file
    Return: sqlite3 connection
    """

    if path is None:
        path = os.path.expanduser("~/.ckpod/ckpod.sqlite")
    conn = sqlite3.connect(path)  # isolation_level=None, timeout=3)
    conn.row_factory = sqlite3.Row
    conn.text_factory = str
    conn.execute("PRAGMA journal_mode=wal")
    return conn


def ensure_config(args):
    """
    Ensure that the config file exists and contains a config file and database

    Arguments: ArgumentParser object (for 'confdir')
    Return: ConfigParser object with config loaded
    Side Effects: may create config directory, a configuration file, and an sqlite database
    """

    path_conf = os.path.join(args.confdir, "ckpod.ini")
    path_db = os.path.join(args.confdir, "ckpod.sqlite")

    if not os.path.exists(args.confdir):
        logging.debug("creating config dir %s", args.confdir)
        os.makedirs(args.confdir)

    dbh = sqlite3.connect(path_db)

    count_tables_sql = """SELECT COUNT(*) FROM sqlite_master WHERE
                          type = "table" and name = "history";"""
    create_table_sql = """
    CREATE TABLE history (
        id INTEGER PRIMARY KEY,
        pub_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        add_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        podname TEXT DEFAULT "",
        filesize INTEGER NOT NULL DEFAULT 0,
        downloaded INTEGER NOT NULL DEFAULT 0,
        title TEXT DEFAULT "",
        url TEXT UNIQUE NOT NULL,
        duration INTEGER NOT NULL DEFAULT 0
    );

    CREATE INDEX idx_downloaded ON history ( downloaded );
    CREATE INDEX idx_pub_time ON history ( pub_time );
    CREATE INDEX idx_add_time ON history ( add_time );
    CREATE INDEX idx_duration ON history ( duration );
    CREATE INDEX idx_podname ON history ( podname );
    """

    num_tables = dbh.execute(count_tables_sql).fetchone()[0]
    if num_tables == 0:
        logging.debug("initializing database %s", path_db)
        with dbh:
            dbh.executescript(create_table_sql)
    dbh.close()
    logging.debug("database is ready")

    global CKPOD_CONFIG
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        logging.debug("Just so you know, SafeConfigParser is deprecated...")
        CKPOD_CONFIG = SafeConfigParser(inline_comment_prefixes=";")

    if not os.path.exists(path_conf):
        logging.debug("generating sample config %s", path_conf)

        sample = """
            [DEFAULT]
            num_parallel_downloads = 1 ; number of simultaneous downloads
            download_limit = 10 ; number of most recent episodes downloaded, 0 = all of them
            destdir = ~/ckpod/%(name)s ; default storage directory

            [example]
            url = https://example.com/podcast/sample.rss?foo=1&bar=2&quux=3 ; feed URL
            destdir = /path/to/podcasts/dir ; default can be overridden per-podcast
            transform = s/a/b/ ; use as many as you need
        """
        with open(path_conf, "w") as out_fd:
            out_fd.write(sample)

    CKPOD_CONFIG.read(unicode(path_conf))
    sections = CKPOD_CONFIG.sections()
    if sections == [u"example"]:
        logging.fatal("review and edit the configuration in %s", path_conf)
    logging.debug("config file sections: %s", sections)

    for (
        name
    ) in CKPOD_CONFIG.sections():  # hack to allows "%(name)" interpolations on python2
        CKPOD_CONFIG.set(name, "name", name)

    return CKPOD_CONFIG


def download_episode_list(dl_args):
    """
    Download the current episode list for a podcast

    Arguments: dl_args - a tuple of (podname, podcast_url)
    Return: tuple of (podcast_name, download_status)
    Side Effects: loads episodes into the database
    """

    global CKPOD_CONFIG
    global DB_LOCK

    if not CKPOD_CONFIG.getboolean(dl_args[0], "enabled"):
        logging.debug("%s: feed not enabled", dl_args[0])
        return (dl_args[0], "skip")

    try:
        resp = requests.get(dl_args[1])
    except requests.ConnectionError:
        logging.warning("%s: caught ConnectionError", dl_args[0])
        return (dl_args[0], False)

    if not resp.ok:
        logging.debug(
            "%s: HTTP/%d while downloading %s", dl_args[0], resp.status_code, resp.url
        )
        return (dl_args[0], False)

    try:
        pod = Podcast(resp.content)
    except TypeError, cur_ex:
        logging.warning("%s: caught exception %s", dl_args[0], cur_ex)
        return (dl_args[0], False)

    if not pod.is_valid_podcast:
        logging.warning("%s: couldn't parse podcast", dl_args[0])
        return (dl_args[0], False)

    logging.info("%s: %s items", dl_args[0], len(pod.items))

    episodes = []
    epoch = arrow.get("0:00", "m:ss")
    for episode in pod.items:
        try:
            duration = (
                arrow.get(episode.itunes_duration, ["H:mm:ss", "mm:ss", "m:ss", "s"])
                - epoch
            )
            duration = int(duration.total_seconds())
        except ValueError as e:
            if "minute must be in" in str(e.args):
                m, s = episode.itunes_duration.split(":")
                duration = 60 * int(m) + int(s)
        except TypeError:
            duration = 0

        try:
            pubtime = arrow.get(
                episode.published_date, ["D MMM YYYY HH:mm:ss", "D MMMM YYYY HH:mm:ss"]
            ).datetime
        except arrow.parser.ParserError:
            logging.warning(
                "%s: unable to parse episode publish date '%s'. Using current time.",
                dl_args[0],
                episode.published_date,
            )
            pubtime = arrow.get().datetime

        episodes.append(
            (
                dl_args[0],
                episode.enclosure_url,
                episode.title.encode("utf-8"),
                episode.enclosure_length,
                pubtime,
                duration,
            )
        )

    insert_sql = """INSERT OR IGNORE INTO history
                 (podname, url, title, filesize, pub_time, duration)
                 VALUES (?, ?, ?, ?, ?, ?)"""
    thread_dbh = dbconnect()
    with DB_LOCK:
        thread_dbh.executemany(insert_sql, episodes)
        thread_dbh.commit()
    return (dl_args[0], True)


def download_episode(episode):
    """
    Download an episode

    Arguments: episode - a dict
    """

    global CKPOD_CONFIG
    global DB_LOCK
    global program_args

    podname = episode["podname"]
    if not CKPOD_CONFIG.getboolean(podname, "enabled"):
        return episode["url"], "skip_feed"

    if "?" in episode["url"]:
        remote_name, query_string = os.path.basename(episode["url"]).split("?")
    else:
        remote_name = os.path.basename(episode["url"])
        query_string = ""

    params = dict(urlparse.parse_qsl(query_string))
    params["remote_name"] = remote_name
    base, ext = os.path.splitext(remote_name)
    params["basename"] = base
    params["ext"] = ext
    params.update(episode)  # merge in job properties

    if CKPOD_CONFIG[podname]["sed"]:
        sed_search, sed_replace, _ = re.match(
            r"""^s(.)(.+?)\1(.+?)\1(.+?)?$""", CKPOD_CONFIG[podname]["sed"]
        ).groups()[1:4]
        new_name = re.sub(sed_search, sed_replace, episode["url"])
        params["remote_name"] = new_name

    disk_file_name = os.path.expanduser(
        os.path.join(CKPOD_CONFIG[podname]["destdir"], params["remote_name"])
    )
    logging.debug("%s -> %s", params["podname"], disk_file_name)

    if CKPOD_CONFIG.getboolean(podname, "dry_run"):
        return episode["url"], "dry_run"

    if not os.path.exists(CKPOD_CONFIG[podname]["destdir"]):
        try:
            os.makedirs(os.path.expanduser(CKPOD_CONFIG[podname]["destdir"]))
        except OSError:
            pass
    dbh = dbconnect()

    if os.path.exists(disk_file_name):
        file_size = os.path.getsize(disk_file_name)
        if file_size == episode["filesize"]:
            logging.debug("download complete: %s", disk_file_name)
            with DB_LOCK:
                with dbh:
                    query = "UPDATE history SET downloaded=1 WHERE podname=? AND url=?"
                    dbh.execute(query, (podname, episode["url"]))
            return episode["url"], True
    else:
        file_size = 0

    resume_header = {"Range": "bytes=%d-" % file_size}
    resp = None
    try:
        resp = requests.get(
            episode["url"], stream=True, headers=resume_header, timeout=10
        )
    except requests.ReadTimeout:
        return episode["url"], False

    status = False
    downloaded = -1
    if resp.ok:
        with open(disk_file_name, "ab") as out_fd:
            for chunk in resp.iter_content(16384):
                if chunk:
                    out_fd.write(chunk)
        status = True
        downloaded = 1

    with DB_LOCK:
        with dbh:
            query = "UPDATE history SET downloaded=? WHERE podname=? AND url=?"
            dbh.execute(query, (downloaded, podname, episode["url"]))
    return episode["url"], status, resp.status_code


def probe_feed():
    global program_args
    resp = requests.get(program_args.probe)
    if resp.ok is False:
        print "HTTP/{} - failed to probe {}".format(
            resp.status_code, program_args.probe
        )
        exit(1)

    pod = Podcast(resp.content)
    if pod.is_valid_podcast is False:
        print "Invalid feed: {}".format(program_args.probe)
        exit(1)

    try:
        for episode in pod.items:
            resp = requests.get(
                episode.enclosure_url, timeout=program_args.timeout, stream=True
            )
            if program_args.sed:
                sed_search, sed_replace, _ = re.match(
                    r"""^s(.)(.+?)\1(.+?)\1(.+?)?$""", program_args.sed
                ).groups()[1:4]
                new_name = re.sub(sed_search, sed_replace, episode.enclosure_url)
                print new_name
            if resp.ok:
                print episode.enclosure_url
                print resp.url
                resp.close()
            print ""
    except KeyboardInterrupt:
        return


def main():
    global CKPOD_CONFIG
    global DB_LOCK
    global program_args

    parse_args()  # modifies program_args
    logfmt = "%(levelname)s: %(message)s"
    loglevel = logging.WARN
    if program_args.verbose:
        if program_args.verbose > 1:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO

    logging.basicConfig(format=logfmt, level=loglevel)
    CKPOD_CONFIG = ensure_config(program_args)

    if program_args.probe:
        probe_feed()
        exit(0)

    dbh = dbconnect()

    feeds = filter(
        lambda section_name: section_name not in ["example", "DEFAULT"],
        CKPOD_CONFIG.sections(),
    )
    feed_urls = map(
        lambda feed_name: (feed_name, CKPOD_CONFIG[feed_name]["url"]), feeds
    )

    workers = ThreadPool(program_args.downloads)
    logging.info(
        "Refreshing %d feeds with %d threads", len(feed_urls), program_args.downloads
    )
    workers.map(download_episode_list, feed_urls, chunksize=1)

    jobs = None
    with DB_LOCK:
        with dbh:
            query = "SELECT * FROM history WHERE downloaded=0 ORDER BY pub_time DESC"
            jobs = map(dict, dbh.execute(query).fetchall())

    if program_args.refresh:
        logging.info("Found %d new episodes", len(jobs))
        return

    logging.info(
        "Downloading %d episodes with %d threads", len(jobs), program_args.downloads
    )
    workers.map(download_episode, jobs, chunksize=1)


if __name__ == "__main__":
    main()
