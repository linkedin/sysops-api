#!/usr/bin/python2.6

# (c) [2013] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import os
import sys
import subprocess
import signal
import redis
import hashlib
import threading
import Queue
import platform
sys.path.append("/usr/local/admin")
import RedisFinder
import seco.range
import bz2
import time

class CacheExtractor(RedisFinder.RedisFinder):
  def __init__(self,
		scope = None,
		site = None,
		range_servers = None,
		range_query = None,
                search_string = None,
		return_randomized_servers = True,
		list_files = False,
		prefix_hostnames = False,
		verbose = False,
		md5sum = False,
		stat = False,
                wordcount = False,
		contents = False):

    self._cm_conf = open('/etc/cm.conf','r')
    self._redis_corelist = []
    self._object_store = {}
    self._named_object_results = {}
    self._gold = {}
    self._number_of_results = 0
    self._verbose = verbose
    self._list_files = list_files
    self._md5sum = md5sum
    self._contents = contents
    self._stat = stat
    self._wordcount = wordcount
    self._search_string = search_string
    self._prefix_hostnames = prefix_hostnames
    self._return_randomized_servers = return_randomized_servers

    # We used to use redis database 0, which was plain text.  Now, bz2 compression populates database 1.
    self._database = 1

    # value at array index 0 is the contents of the file itself.
    # value at array index 1 is the md5sum of the file
    # value at array index 2 is the os.stat contents of the file.
    # value at array index 3 is the wordcount of the file
    self._index_contents = 0
    self._index_md5sum = 1
    self._index_stat = 2
    self._index_wordcount = 3

    self._range_servers = []
    if range_servers is not None:
      for rs in range_servers.split(','):
        self._range_servers.append(rs)

    if scope:
      self._scope = scope
    else:
      self._scope = 'local'

    if site:
      self._site = site
      self._scope = 'site'
    else:
      self._site = self.discover_site()
    
    if range_query:
      self._range_query = range_query
      # If the user specified a --site on the CLI, allow that option to be preserved.  global queries are expensive to search against.
      # In the chance that the data isn't found on the site specified on the CLI, we throw an execption.  Otherwise, execution is much faster.
      if self._scope != 'site':
        self._scope = 'global'
    else:
      self._range_query = None

    if self._list_files:
      self._search_string = '/'

    if self._search_string:
      if "#" not in self._search_string:
        self._search_string = '*' + self._search_string + '*'

    # The below statement fires off the work in RedisFinder.RedisServes to generate our corelist.
    # The CacheExtractor object inherits functions in RedisFinder where this work is being processed.
    self.query_range_for_redis_corelist()

    self._redis_servers = self.get_redis_corelist()

    if self._verbose:
      print "(+) CacheExtractor __init__  scope", self._scope
      print "(+) CacheExtractor __init__  range_query", self._range_query
      print "(+) CacheExtractor __init__  database", self._database
      print "(+) CacheExtractor __init__  redis_corelist", self._redis_corelist
      print "(+) CacheExtractor __init__  object_store", self._object_store
      print "(+) CacheExtractor __init__  named_object_results", self._named_object_results
      print "(+) CacheExtractor __init__  site", self._site
      print "(+) CacheExtractor __init__  list_files", self._list_files
      print "(+) CacheExtractor __init__  prefix_hostnames", self._prefix_hostnames
      print "(+) CacheExtractor __init__  md5sum", self._md5sum
      print "(+) CacheExtractor __init__  contents", self._contents
      print "(+) CacheExtractor __init__  stat", self._stat
      print "(+) CacheExtractor __init__  wordcount", self._wordcount
      print "(+) CacheExtractor __init__  search_string", self._search_string
      print "(+) CacheExtractor __init__  return_randomized_servers", self._return_randomized_servers
      print "(+) CacheExtractor __init__  cm.conf", self._cm_conf
      print "(+) CacheExtractor __init__  range_servers", self._range_servers
      print "(+) CacheExtractor __init__  redis_servers", self._redis_servers
   
    # Do actual work.
    if self._search_string:
      self.list_of_matching_named_objects()
      self.extract_named_objects()

###############################################################################################
  def print_redis_server_information(self):
    for redis_server in self._redis_servers:
      redis_connection = redis.Redis(host=redis_server,port=6379,db=self._database,socket_timeout=5,charset='utf-8', errors='strict')
      redis_info = redis_connection.info()
      for key in redis_info.iterkeys():
        if self._prefix_hostnames:
          print redis_server.ljust(30) + "\t" + key.ljust(50) + "\t" + str(redis_info[key]).ljust(80).strip()
        else:
          print key.ljust(50) + str(redis_info[key]).rjust(50).strip()
###############################################################################################
  def list_of_matching_named_objects(self):
    """
    Populates a dictionary containing a list of named_objects per redis server.  If range query has been passed on the CLI,
    then we only look for named_objects from hosts within that range.  Otherwise, we go 'global' in scope, and return named_objects
    for all hosts.  Range is actually a restrictive operation here, not an inclusive one.
    """

    ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ##
    def threaded_object_finder(queue, redis_server):
      try:
        redis_connection = redis.Redis(host=redis_server,port=6379,db=self._database,socket_timeout=5,charset='utf-8', errors='strict')
        queue.put(sorted(redis_connection.keys(self._search_string)))
      except redis.exceptions.ResponseError, e:
        print "CacheExtractor.list_of_matching_named_objects().threaded_object_finder() Exception " + str(e)
        os._exit(1)
    ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ##

    queue = Queue.Queue()
    threads = []
    for redis_server in self._redis_servers:
      thread = threading.Thread(target=threaded_object_finder, args=(queue, redis_server))
      threads.append(thread)
      thread.start()
      self._named_object_results[redis_server] = queue.get()  
      for named_object in self._named_object_results[redis_server]:
        self._number_of_results += 1
        if self._verbose:
          print "(+) CacheExtractor.list_of_matching_named_objects() named_object " + named_object + " discovered from redis server " + redis_server

    for thread in threads:
      thread.join()

    if self._number_of_results == 0:
      raise Exception('No results were found from the search.  Please try your search again.  use --list-files to get an idea of what objects exist in the cache.')

    # When we insert keys into the cache, we insert with <hostname>$<uuid>.  At the end of insertion, we rename the key from this uuid to the actual key name.
    # The actual key name will be in the form <hostname>#<filename>
    # This allows the operation to be atomic.  We can either search and find the object, or we can't.  Before, there was a race condition where we could be extracting
    # the key at the exact moment of insertion.  If we dont find a key with "#" in the name of the key, remove it from results.  We shouldn't be searching against
    # objects that dont contain # in the keyname.  
    temp_results = {}
    for redis_server in self._redis_servers:
      temp_results[redis_server] = []
      for named_object in self._named_object_results[redis_server]:
        if "#" in named_object:
          temp_results[redis_server].append(named_object)
        else:
          if self._verbose: 
            print "(+) CacheExtractor.list_of_matching_named_objects() named_object " + named_object + " removed from redis server " + redis_server
      self._named_object_results[redis_server] = temp_results[redis_server]
      
    if self._range_query:
      for range_server in self._range_servers:
        if self._verbose:
          print "(+) CacheExtractor.list_of_matching_named_objects() range_server is ", range_server
          print "(+) CacheExtractor.list_of_matching_named_objects() self.range_query is ", self._range_query
        try:
          range_connection = seco.range.Range(range_server)
          range_results = range_connection.expand(self._range_query)
          if range_results:
            break
        except seco.range.RangeException:
          print "(+) CacheExtractor.list_of_matching_named_objects() range query invalid"
          sys.exit(1)

      temp_results = {}
      for redis_server in self._redis_servers:
        temp_results[redis_server] = []
        for named_object in self._named_object_results[redis_server]:
          for range_result in range_results:
            if range_result in named_object:
              temp_results[redis_server].append(named_object) 
        self._named_object_results[redis_server] = temp_results[redis_server]
###############################################################################################
  def extract_named_objects(self):

    threads = []
    for redis_server in self._redis_servers:
      thread = threading.Thread(target=self.threaded_object_extractor, args=(redis_server,))
      threads.append(thread)
      thread.start()

    for thread in threads:
      thread.join()
###############################################################################################
  def threaded_object_extractor(self, redis_server):
      redis_connection = redis.Redis(host=redis_server,port=6379,db=self._database,socket_timeout=5,charset='utf-8', errors='strict')
      redis_pipeline = redis_connection.pipeline()

      try:
        for named_object in self._named_object_results[redis_server]:
          if self._contents:
            redis_pipeline.lindex(named_object, self._index_contents) 
          elif self._md5sum:
            redis_pipeline.lindex(named_object, self._index_md5sum)
          elif self._stat:
            redis_pipeline.lindex(named_object, self._index_stat)
          elif self._wordcount:
            redis_pipeline.lindex(named_object, self._index_wordcount)

        # This lies outside the loop of named_objects.  The pipeline.execute() below will issue a single redis query to fetch everything at once.
        # By using pipelines instead of individual fetches, this reduces cross communcation between the client and server.  See here for more details.
        # https://github.com/andymccurdy/redis-py 
        self._object_store[redis_server] = []
        self._object_store[redis_server] = redis_pipeline.execute()

      except redis.exceptions.ResponseError, e:
        print "CacheExtractor.threaded_object_extractor() Exception " + str(e)
        sys.exit(1)

      # The named objects array and object store arrays should be a 1-to-1 mapping at this point.  Iterate over both arrays in parallel.
      # named_object_results[redis_server] = names of the keys
      # object_store[redis_server] = whatever we extracted from the redis server for all of the keys
      # gold[name of key] = whatever we extracted
      uniques = {}
      while self._named_object_results[redis_server]:
        named_object = self._named_object_results[redis_server].pop()
        host, file = named_object.split('#')
        if self._object_store[redis_server]:
          # We are in data extraction mode with contents, md5sum, stat, or wordcount.
          contents_of_named_object = self._object_store[redis_server].pop()
          if contents_of_named_object:
            self._gold[named_object] = bz2.decompress(contents_of_named_object)
        else:
          # We are either in --search or --list-files operations.  We didn't actually extract data.  if we are in --list-files, we want a list of unique
          # objects, so we build a dictionary to perform the uniques for us.
          if self._list_files:
            uniques[file] = 1
          else:
            # We are searching, so we want non-unique filename objects across multiple hosts
            self._gold[named_object] = None

        if self._verbose:
          print "(+) CacheExtractor.threaded_object_extractor() file " + file + " from host " + host + " discovered from redis server " + redis_server

      if self._list_files:
        for file in uniques.iterkeys():
          self._gold['files#' + file] = None
        
###############################################################################################
