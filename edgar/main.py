# -*- coding: utf-8 -*-
from __future__ import print_function
import multiprocessing
import os
import datetime
import zipfile
import tempfile
import logging
import os.path
import sys
import io
import time

EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEP = "|"
IS_PY3 = sys.version_info[0] >= 3
MAX_NO_OF_CALLS_PER_PERIOD = 10
PERIOD = datetime.timedelta(seconds=1)


def _get_current_quarter():
    return "QTR%s" % ((datetime.date.today().month - 1) // 3 + 1)


def _quarterly_idx_list(since_year=1993):
    """
    Generate the list of quarterly zip files archived in EDGAR
    since 1993 until this previous quarter
    """
    logging.debug("downloading files since %s" % since_year)
    years = range(since_year, datetime.date.today().year + 1)
    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
    history = list((y, q) for y in years for q in quarters)
    history.reverse()

    quarter = _get_current_quarter()

    while history:
        _, q = history[0]
        if q == quarter:
            break
        else:
            history.pop(0)

    return [
        (
            EDGAR_PREFIX + "edgar/full-index/%s/%s/master.zip" % (x[0], x[1]),
            "%s-%s.tsv" % (x[0], x[1]),
        )
        for x in history
    ]


def _append_html_version(line):
    chunks = line.split(SEP)
    return line + SEP + chunks[-1].replace(".txt", "-index.html")


def _skip_header(f):
    for x in range(0, 11):
        f.readline()


def _url_get(url, user_agent):
    content = None
    if IS_PY3:
        # python 3
        import urllib.request
        hdr = { 'User-Agent' : user_agent }
        req = urllib.request.Request(url, headers=hdr)
        content =urllib.request.urlopen(req).read()
    else:
        # python 2
        import urllib2

        content = urllib2.urlopen(url).read()
    return content


def _download(file, dest, skip_file, user_agent):
    """
    Download an idx archive from EDGAR
    This will read idx files and unzip
    archives + read the master.idx file inside

    when skip_file is True, it will skip the file if it's already present.
    """
    if not dest.endswith("/"):
        dest = "%s/" % dest

    url = file[0]
    dest_name = file[1]
    if skip_file and os.path.exists(dest+dest_name):
        logging.info("> Skipping %s" % (dest_name))
        return

    if url.endswith("zip"):
        with tempfile.TemporaryFile(mode="w+b") as tmp:
            tmp.write(_url_get(url, user_agent))
            with zipfile.ZipFile(tmp).open("master.idx") as z:
                with io.open(dest + dest_name, "w+", encoding="utf-8") as idxfile:
                    _skip_header(z)
                    lines = z.read()
                    if IS_PY3:
                        lines = lines.decode("latin-1")
                    lines = map(
                        lambda line: _append_html_version(line), lines.splitlines()
                    )
                    idxfile.write("\n".join(lines)+"\n")
                    logging.info("> downloaded %s to %s%s" % (url, dest, dest_name))
    else:
        raise logging.error("python-edgar only supports zipped index files")


def download_index(dest, since_year, user_agent, skip_all_present_except_last=False):
    """
    Convenient method to download all files at once
    """
    if not os.path.exists(dest):
        os.makedirs(dest)

    tasks = _quarterly_idx_list(since_year)
    logging.info("%d index files to retrieve", len(tasks))
    
    # make list of processes to be run in parallel, first should be always downloaded
    processes = [multiprocessing.Process(target = _download, args=(file, dest, False, user_agent)) if i == 0 else \
                multiprocessing.Process(target = _download, args=(file, dest, skip_all_present_except_last, user_agent)) \
                for i, file in enumerate(tasks) ]
    # group processes into sublists of maximum number of calls per period
    processes = [ processes[i:i+MAX_NO_OF_CALLS_PER_PERIOD] for i in range(0, len(processes), MAX_NO_OF_CALLS_PER_PERIOD)]
    
    
    for process_group in processes:
        Start = datetime.datetime.now()
        for p in process_group:
            p.start()
        # if max alowed number of calls launched sooner than given period of time, 
        # then wait until end of period 
        Stop = datetime.datetime.now()
        if Stop < (Start + PERIOD):
            Wait_time = ((Start + PERIOD)-Stop).total_seconds()
            time.sleep(Wait_time)
            logging.info(f"sleeping for {Wait_time*1000} ms because we are going too fast, "
                + f"previous {MAX_NO_OF_CALLS_PER_PERIOD} downloads were spawned in {(Stop - Start).total_seconds()*1000} ms")
        
        for p in process_group:
            p.join()

    logging.info("complete")
