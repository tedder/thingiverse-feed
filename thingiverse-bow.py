#!/usr/bin/env python3

## COPYRIGHT, MIT license
#
# Copyright 2018 tedder.me
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
#

import boto3
import requests
import sys
from datetime import datetime,timedelta,date,timezone
#import datetime.tzinfo.timezone
import json
import pymysql
import warnings
import os

BASE_URL = 'https://api.thingiverse.com/users/KryptonicLoser/collections'
DEBUG=0
DEBUG_S3=0

def get_secret(secret_name):
  sc = boto3.client('secretsmanager', region_name='us-east-1')
  sc_ret = sc.get_secret_value( SecretId=secret_name )

  if 'SecretString' in sc_ret:
    if DEBUG > 12: print(sc_ret)
    secret = sc_ret['SecretString']
    return secret
  else:
    if DEBUG > 11: print(sc_ret)
    binary_secret_data = sc_ret['SecretBinary']
    return binary_secret_data

class ThingDB:
  def close(self):
    if self.db:
      self.db.commit()
      self.db.close()
      self.db = None

  def __init__(self):
    self.db = None
    self.dbsec = None
    self.get_db()

  def have_column(self, table, column):
    try:
      d = self.get_db()
      c = self.db.cursor()
      c.execute('SELECT {} FROM {} LIMIT 1'.format(column, table))
      c.fetchone()
      # must be okay
      return True

    except sqlite3.OperationalError:
      return None # this is expected

  def add_column(self, table, column, column_def):
    if self.have_column(table, column):
      return # nothing to do
    d = self.get_db()
    c = self.db.cursor()
    c.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, column, column_def))
    d.commit()

  def get_db(self):
    # versions needed:
    # >= 3.3 for 'if not exists'
    #print(sqlite3.sqlite_version) -> '3.18.0'
    #print(sqlite3.sqlite_version_info) -> (3, 18, 0)

    if self.db:
      # ping throws, so we don't actually care what it returns
      self.db.ping(reconnect=True)
      return self.db

    self.dbsec = json.loads(get_secret('serverless_mysql_1'))
    self.db = pymysql.connect(host=self.dbsec['host'], user=self.dbsec['username'], password=self.dbsec['password'],
      autocommit = True, db='', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')

    with warnings.catch_warnings():
      warnings.simplefilter("ignore")
      c = self.db.cursor()
      c.execute('CREATE DATABASE IF NOT EXISTS thingiverse')
      self.db.commit()
    self.db.select_db('thingiverse')

    with warnings.catch_warnings():
      warnings.simplefilter("ignore")
      c = self.db.cursor()
      c.execute('''CREATE TABLE IF NOT EXISTS items
      (itemid BIGINT NOT NULL PRIMARY KEY, title VARCHAR(255), first_seen DATETIME NOT NULL)
      ''')
      self.db.commit()

    return self.db

  def item_seen(self, itemid):
    #print("iid: {}".format(type(itemid)))
    c = self.get_db().cursor()
    c.execute('''SELECT first_seen FROM items WHERE itemid=%s''', (int(itemid),) )
    dt = c.fetchone()
    if DEBUG > 1: print(dt)

    # might be none, which is okay.
    if not dt: return None

    #print("dt at db: ", dt)
    return dt.get('first_seen').strftime('%Y-%m-%d %H:%M:%S')
    #dtp = dtparser.parse(dt).replace(tzinfo=timezone.utc)
    #print("post parse: ", dtp)
    #return dtp

  def insert_item(self, row):
    c = self.get_db().cursor()
    s = self.item_seen(row['itemid'])
    if s: return s

    if not row.get('first_seen'):
      #row['first_seen'] = datetime.now(timezone.utc).replace(microsecond=0).isoformat() # py3.6 timespec='seconds')
      row['first_seen'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    if DEBUG > 1: print(row['first_seen'])
    #print("a", {**row})
    c.execute('''INSERT INTO items
      (itemid,  title,  first_seen) VALUES
      (%(itemid)s, %(title)s, %(first_seen)s)
      ''', {**row})
    self.get_db().commit()
    return row['first_seen']


class ThingDB_sqlite:

  def close(self):
    if self.db:
      self.db.commit()
      self.db.close()
      self.db = None

  def __init__(self):
    self.db = None
    self.get_db()

  def have_column(self, table, column):
    try:
      d = self.get_db()
      c = self.db.cursor()
      c.execute('SELECT {} FROM {} LIMIT 1'.format(column, table))
      c.fetchone()
      # must be okay
      return True

    except sqlite3.OperationalError:
      return None # this is expected

  def add_column(self, table, column, column_def):
    if self.have_column(table, column):
      return # nothing to do
    d = self.get_db()
    c = self.db.cursor()
    c.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, column, column_def))
    d.commit()

  def get_db(self):
    # versions needed:
    # >= 3.3 for 'if not exists'
    #print(sqlite3.sqlite_version) -> '3.18.0'
    #print(sqlite3.sqlite_version_info) -> (3, 18, 0)

    if self.db:
      return self.db

    pypath = os.path.dirname(__file__)
    dbpath = os.path.join(pypath, 'thingiverse-bom.sqlite3')
    #print("path: {}".format(dbpath))
    if not os.path.isfile(dbpath):
      raise Exception("we expect our sqlite3 db to exist ({}). If it doesn't, maybe it's in the wrong place?".format(dbpath))
    self.db = sqlite3.connect(dbpath)
    c = self.db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS items
      (itemid NUMBER, title TEXT, first_seen TEXT)
      ''')
    #c.execute('''CREATE TABLE IF NOT EXISTS state (state_name TEXT, state_value TEXT)''');
    self.db.commit()
    #self.add_column('spots', 'band', 'NUMBER')
    return self.db

  def item_seen(self, itemid):
    #print("iid: {}".format(type(itemid)))
    c = self.get_db().cursor()
    c.execute('''SELECT first_seen FROM items WHERE itemid=?''', (int(itemid),) )
    ret = c.fetchone()

    # might be none, which is okay.
    if not ret: return None
    dt = ret[0]

    # might be none, which is okay.
    if not dt: return None

    #print("dt at db: ", dt)
    return dt
    #dtp = dtparser.parse(dt).replace(tzinfo=timezone.utc)
    #print("post parse: ", dtp)
    #return dtp

  def insert_item(self, row):
    c = self.get_db().cursor()
    s = self.item_seen(row['itemid'])
    if s: return s

    if not row.get('first_seen'):
      row['first_seen'] = datetime.now(timezone.utc).replace(microsecond=0).isoformat() # py3.6 timespec='seconds')
    #print("a", {**row})
    c.execute('''INSERT INTO items
      (itemid,  title,  first_seen) VALUES
      (:itemid, :title, :first_seen)
      ''', {**row})
    self.get_db().commit()
    return row['first_seen']

def json_serial(obj):
  """JSON serializer for objects not serializable by default json code"""

  if isinstance(obj, (datetime, date)):
    return obj.isoformat()
  raise TypeError("Type %s not serializable" % type(obj))

def parsedate(datestr):
  #2018-01-31T17:33:51+00:00
  # 2018-01-10T23:24:40+00:00
  # note +00:00 is not %z because of the :

  try:
    # mysql
    return datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')
  except ValueError:
    pass

  try:
    return datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S+00:00').replace(tzinfo=timezone.utc)
  except ValueError:
    pass

  try:
    # yes tz, yes millisec
    return datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S.%f+00:00').replace(microsecond=0).replace(tzinfo=timezone.utc)
  except ValueError:
    pass

  try:
    # no tz
    return datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
  except ValueError:
    pass

  try:
    # no tz, yes millisec
    return datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S.%f').replace(microsecond=0).replace(tzinfo=timezone.utc)
  except ValueError:
    pass

  # else

  raise Exception("could not parse date: {}".format(datestr))


def make_request(url, params={}):
  #thingiverse_api:auth_token
  thingiverse_secrets = get_secret('thingiverse_api')
  auth_token = json.loads(thingiverse_secrets).get('auth_token')
  if not auth_token:
    raise Exception("could not get auth_token from secrets manager: {}".format(auth_token))

  req = requests.get(url, headers={'Authorization': 'Bearer {}'.format(auth_token)}, params=params)
  #print(req.headers['link'])
  #sys.exit(0)

  return req.json()

def get_recent_collection_items(db, apiurl, collection_name):
  retitems = []

  things = []
  pagenum = 1
  while True:
    this_set = make_request(apiurl + "/things", params={'sort': 'date', 'order': 'desc', 'page': pagenum})
    if DEBUG: print("page: ", pagenum, " set size: ", len(this_set))
    if len(this_set) == 0:
      break
    things.extend(this_set)
    pagenum += 1

  for thing in things:
    #print("xx", json.dumps(itemjson, indent=2))
    #thing["url"]: "https://api.thingiverse.com/things/2788192",
    #row['first_seen]' = datetime.now.isoformat() # py3.6 timespec='seconds')
    if DEBUG > 1: print(int(thing['id']), thing['name'])
    seen = db.insert_item({'itemid': int(thing['id']), 'title': thing['name']})
    #seen_dt = seen.replace(tzinfo=timezone.utc)
    seen_dt = parsedate(seen).replace(tzinfo=timezone.utc)
    #print("{} / {}  vs {}".format(seen, seen_dt, datetime.now(timezone.utc).replace(microsecond=0)-timedelta(days=7)))
    try:
      if seen_dt < datetime.now(timezone.utc).replace(microsecond=0)-timedelta(days=7):
        #print("too old: {} / {}".format(seen_dt, datetime.now(timezone.utc).replace(microsecond=0)-timedelta(days=7)))
        continue
    except TypeError:
        #mismatched date types: 2018-06-02 05:04:34 -- 2018-06-02 05:04:34 / 2018-05-26 05:04:34+00:00
      print("mismatched date types: {} -- {} / {}".format(seen, seen_dt, datetime.now(timezone.utc).replace(microsecond=0)-timedelta(days=7)))

    formatstrs =  { 'name': thing['name'], 'thumbnail': thing['thumbnail'], 'creator_name': thing['creator']['name'] }
    oneitem = {
      'id': thing['id'],
      'url': thing['public_url'],
      'title': '{}: {}'.format(collection_name, thing['name']),
      'content_html': '{name} by {creator_name}<br><img src="{thumbnail}">'.format(**formatstrs),
      'content_text': '{name} by {creator_name}'.format(**formatstrs),
      'image': thing['thumbnail'],
      'date_published': seen,
      'author': {
        'name': thing['creator']['name'],
        'url': thing['creator']['public_url'],
        'avatar': thing['creator']['thumbnail'],
      },
    }
    #print(json.dumps(oneitem))
    retitems.append(oneitem)
  #print(json.dumps(retitems))
  return retitems


db = ThingDB()
s3key = 'rss/thingiverse_kryptonicloser_recent_collected.json'
items = []

collections = []
pagenum = 1
while True:
  if DEBUG > 1: print("requesting page: ", pagenum)
  this_collection_set = make_request(BASE_URL, {'page': pagenum})
  if DEBUG: print("page: ", pagenum, " set size: ", len(this_collection_set))
  if len(this_collection_set) == 0:
    break
  collections.extend(this_collection_set)
  pagenum += 1

for collection in collections:
  #print(collection)
  last_modified = parsedate(collection['modified'])
  if DEBUG: print(collection.get('Name'), last_modified, datetime.now(timezone.utc))
  if last_modified.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc)-timedelta(days=14):
    if DEBUG: print("too old")
    continue

  if collection['name'].lower().startswith(('best of', 'bow')): # and  '2/10' in collection['name']:
    last_modified = parsedate(collection['modified'])
    if DEBUG: print(collection['url'], collection['modified'], collection['name'])
    #sys.stderr.write("{} {} {}\n".format(collection['url'], collection['modified'], collection['name']))
    colitems = get_recent_collection_items(db, collection['url'], collection['name'])
    items += colitems
    #break

jsonblob = {
  'version': 'https://jsonfeed.org/version/1',
  'items': items,
  'title': 'recent items collected by KryptonicLoser',
  'home_page_url': 'https://www.thingiverse.com/KryptonicLoser/about',
  'feed_url': 'https://dyn.tedder.me/' + s3key,
  'user_comment': "items from recent collections of KryptonicLoser. Designed to get the Best-of-Week items.",
  'author': { 'name': 'tedder', 'url': 'https://tedder.me/' },
}

if DEBUG_S3:
  with open(os.path.basename(s3key) or 'bow.json', 'w') as f:
    f.write(json.dumps(jsonblob, indent=2))
else:
  s3 = boto3.client('s3')
  #cloudfront_client = boto3.client('cloudfront')
  putret = s3.put_object(
    Body=json.dumps(jsonblob, indent=2),
    ACL='public-read',
    Bucket='dyn.tedder.me',
    Key=s3key,
    CacheControl='max-age=6',
    ContentType='application/json',
  )
#print(putret['ETag'])
#print(json.dumps(jsonblob, indent=2))
#print(json.dumps(items, default=json_serial, indent=2))
#curl -X GET --header "Authorization: Bearer 6f9d1fffeb271181861cae288ce07527" https://api.thingiverse.com/users/KryptonicLoser/collections | python3 -m json.tool
#jq '.[]|[.modified,.name,.url]'


