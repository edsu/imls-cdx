#!/usr/bin/env python

"""
Reads original IMLS CSV file and looks up website URL in the redis
database that was populated with CDX files with scripts/index.py

Caveats:
- it only outputs museums who have both an income and a website url
- it only looks up website URLs that are at a domain witout a path
- a museum with 0 pages might just mean the crawler hasn't reached it yet

Ideas:
- 200s / total pages
- 404s / total pages
- 500s / total pages
- total pages / income
- data size / income
- data size / total pages (average page size)
- sites with robots that have 1 or 2 pages?
- inbound links from outside the website (page rank)

"""

import re
import csv
import sys
import surt
import redis
import urlparse

from os.path import dirname, join

r = redis.StrictRedis()

w = csv.writer(sys.stdout)
w.writerow([
    'Name',
    'URL',
    'Income',
    'Pages',
    'Size',
    '404',
    '500',
    'Robots'
])

with open('imls-2015.csv') as csvdata:
    for row in csv.reader(csvdata):
        name = row[1]
        url = row[6].lower()
        url = re.sub('^https?://', '', url)
        url = url.rstrip('/')
        if '/' in url: continue

        surt_url = surt.surt(url)
        surt_url = re.sub('\)/$', '', surt_url)

        income = row[11]
        try:
            income = float(income.strip("$").strip(",").strip(" "))
        except ValueError:
            continue

        if not (url and income):
            continue

        pages = int(r.zscore('hosts', surt_url) or 0)
        status_404 = int(r.zscore('status-404', surt_url) or 0)
        status_500 = int(r.zscore('status-500', surt_url) or 0)
        size = int(r.zscore('size', surt_url) or 0)
        robots = r.sismember('robots', surt_url)

        w.writerow([
            name,
            url,
            income,
            pages,
            size,
            status_404,
            status_500,
            robots,
        ])
