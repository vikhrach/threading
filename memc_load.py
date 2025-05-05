#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import glob
import gzip
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from optparse import OptionParser

from pymemcache.client.base import PooledClient

import appsinstalled_pb2

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

device_memc = dict.fromkeys([b"idfa", b"gaid", b"adid", b"dvid"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_client: PooledClient, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_client, key, str(ua).replace("\n", " ")))
        else:
            memc_client.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_client, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split(b"\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(b",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(b",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_line(line):
    processed = errors = 0
    line = line.strip()
    if not line:
        return processed, errors
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        errors += 1
        return processed, errors
    memc_client = device_memc.get(appsinstalled.dev_type)
    if not memc_client:
        errors += 1
        logging.error("Unknow device type: %s" % appsinstalled.dev_type)
        return processed, errors
    ok = insert_appsinstalled(memc_client, appsinstalled)
    if ok:
        processed += 1
    else:
        errors += 1
    return processed, errors


def main(options):
    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        logging.info("Processing %s" % fn)
        fd = gzip.open(fn)
        lines = fd.readlines()

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(process_line, lines))

        processed, errors = tuple(sum(x) for x in zip(*results))
        print(processed, errors)
        if not processed:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    device_memc[b"idfa"] = PooledClient(opts.idfa, max_pool_size=8)
    device_memc[b"gaid"] = PooledClient(opts.gaid, max_pool_size=8)
    device_memc[b"adid"] = PooledClient(opts.adid, max_pool_size=8)
    device_memc[b"dvid"] = PooledClient(opts.dvid, max_pool_size=8)
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO if not opts.dry else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
