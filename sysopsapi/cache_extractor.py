#!/usr/bin/python2.6
# filesource    \$HeadURL: svn+ssh://csvn@esv4-sysops-svn.corp.linkedin.com/export/content/sysops-svn/cfengine/trunk/generic_cf-agent_policies/config-general/manage_usr_local_admin/CacheExtractor.py $
# version       \$Revision: 123922 $
# modifiedby    \$LastChangedBy: msvoboda $
# lastmodified  \$Date: 2014-06-16 15:49:08 -0400 (Mon, 16 Jun 2014) $

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
import sysopsapi.redis_finder
import seco.range
import bz2

# System identifcation
import __main__
import uuid
import pwd
import json


class CacheExtractor(sysopsapi.redis_finder.RedisFinder):

    def __init__(self,
                 scope=None,
                 site=None,
                 range_servers=None,
                 range_query=None,
                 file=None,
                 search_string=None,
                 return_randomized_servers=True,
                 list_files=False,
                 prefix_hostnames=False,
                 verbose=False,
                 cost=False,
                 md5sum=False,
                 stat=False,
                 wordcount=False,
                 contents=False,
                 time=False):

        self._cm_conf = open('/etc/cm.conf', 'r')
        self._redis_corelist = []
        self._object_store = {}
        self._named_object_results = {}
        self._gold = {}
        self._number_of_results = 0
        self._verbose = verbose
        self._cost = cost
        self._list_files = list_files
        self._md5sum = md5sum
        self._contents = contents
        self._stat = stat
        self._wordcount = wordcount
        self._time = time
        self._search_string = search_string
        self._prefix_hostnames = prefix_hostnames
        self._return_randomized_servers = return_randomized_servers
        self._file = file

        # We used to use redis database 0, which was plain text.  Now, bz2
        # compression populates database 1.
        self._database = 1

        # value at array index 0 is the contents of the file itself.
        # value at array index 1 is the md5sum of the file
        # value at array index 2 is the os.stat contents of the file.
        # value at array index 3 is the wordcount of the file
        # value at array index 4 is the cache insertion time of the file
        self._index_contents = 0
        self._index_md5sum = 1
        self._index_stat = 2
        self._index_wordcount = 3
        self._index_time = 4

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
            # In the chance that the data isn't found on the site specified on
            # the CLI, we throw an execption.  Otherwise, execution is much
            # faster.
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
        # The CacheExtractor object inherits functions in RedisFinder where
        # this work is being processed.
        self.query_range_for_redis_corelist()

        # O/S information that gets sent to each MPS for each search.
        self._user = pwd.getpwuid(os.getuid())
        self._uuid = str(uuid.uuid4())
        self._cwd = os.getcwd()
        import time
        if ".linkedin.com" not in platform.node():
            self._hostname = platform.node() + ".linkedin.com"
        else:
            self._hostname = platform.node()

        self.info = {}
        self.info = {}
        self.info['redis_servers'] = {}
        self.info['query'] = {}
        self.info['totals'] = {}

        self.info['totals']['total_bytes_redis_cache_downloaded'] = 0
        self.info['totals']['total_bytes_results_decompressed'] = 0
        self.info['totals']['total_keys_matched'] = 0
        self.info['totals']['total_time_start'] = time.time()
        self.info['totals']['total_time_elapsed'] = None

        self.info['query']['pw_name'] = self._user.pw_name
        self.info['query']['pw_uid'] = self._user.pw_uid
        self.info['query']['pw_gid'] = self._user.pw_gid
        self.info['query']['pw_gecos'] = self._user.pw_gecos
        self.info['query']['pw_dir'] = self._user.pw_dir
        self.info['query']['pw_shell'] = self._user.pw_shell
        self.info['query']['utility'] = __main__.__file__
        self.info['query']['self._scope'] = self._scope
        self.info['query']['self._range_query'] = self._range_query
        self.info['query']['self._database'] = self._database
        self.info['query']['self._redis_corelist'] = self._redis_corelist
        self.info['query']['self._site'] = self._site
        self.info['query']['self._list_files'] = self._list_files
        self.info['query']['self._file'] = self._file
        self.info['query']['self._prefix_hostnames'] = self._prefix_hostnames
        self.info['query']['self._md5sum'] = self._md5sum
        self.info['query']['self._contents'] = self._contents
        self.info['query']['self._stat'] = self._stat
        self.info['query']['self._wordcount'] = self._wordcount
        self.info['query']['self._time'] = self._time
        self.info['query']['self._search_string'] = self._search_string
        self.info['query'][
            'self._return_randomized_servers'] = self._return_randomized_servers
        self.info['query']['self._range_servers'] = self._range_servers
        self.info['query']['self._redis_corelist'] = self._redis_corelist
        self.info['query']['self._hostname'] = self._hostname
        self.info['query']['self._cwd'] = self._cwd

        if self._verbose:
            for key in sorted(self.info['query'].iterkeys()):
                print "(+) CacheExtractor __init__  {0} {1}".format(key, self.info['query'][key])

        # Do actual work.
        if self._search_string:
            self.list_of_matching_named_objects()
            self.extract_named_objects()

        # If requested, query how expensive the given query was.
        if self._cost:
            self._gold = None
            self.display_cost_of_cache()
##########################################################################

    def display_cost_of_cache(self):
        print json.dumps(self.info['totals'], indent=3, sort_keys=True)
        print json.dumps(self.info['redis_servers'], indent=3, sort_keys=True)
##########################################################################

    def print_redis_server_information(self):
        for redis_server in self._redis_corelist:
            redis_connection = redis.Redis(
                host=redis_server, port=6379, db=self._database, socket_timeout=5, charset='utf-8', errors='strict')
            redis_info = redis_connection.info()
            for key in redis_info.iterkeys():
                if self._prefix_hostnames:
                    print redis_server.ljust(30) + "\t" + key.ljust(50) + "\t" + str(redis_info[key]).ljust(80).strip()
                else:
                    print key.ljust(50) + str(redis_info[key]).rjust(50).strip()
##########################################################################

    def list_of_matching_named_objects(self):
        """
        Populates a dictionary containing a list of named_objects per redis server.  If range query has been passed on the CLI,
        then we only look for named_objects from hosts within that range.  Otherwise, we go 'global' in scope, and return named_objects
        for all hosts.  Range is actually a restrictive operation here, not an inclusive one.
        """

        ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ##
        def threaded_object_finder(queue, redis_server):
            try:
                redis_connection = redis.Redis(
                    host=redis_server, port=6379, db=self._database, socket_timeout=5, charset='utf-8', errors='strict')
                try:
                    queue.put(
                        sorted(redis_connection.keys(self._search_string)))
                except Exception, e:
                    print "CacheExtractor.list_of_matching_named_objects().threaded_object_finder() Exception " + str(e)
                    os._exit(1)
            except redis.exceptions.ResponseError, e:
                print "CacheExtractor.list_of_matching_named_objects().threaded_object_finder() Exception " + str(e)
                os._exit(1)
        ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ## ** ##

        queue = Queue.Queue()
        threads = []
        for redis_server in self._redis_corelist:
            thread = threading.Thread(
                target=threaded_object_finder, args=(queue, redis_server))
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
            raise Exception('''No results were found from the search.  Please try your search again.  use --list-files to get an idea of what objects exist in the cache.
Its possible that you are in the wrong scope.  This utility uses a local scope by default.
You might want to be using --scope site, --scope global, or --site <datacenter> to adjust your scope.
https://iwww.corp.linkedin.com/wiki/cf/display/IST/Extracting+the+sysops+cache+for+fun+and+profit#Extractingthesysopscacheforfunandprofit-Levelsofscope

If executing extract_sysops_cache.py, use --help for some basic examples of scope.''')

        # When we insert keys into the cache, we insert with <hostname>$<uuid>.  At the end of insertion, we rename the key from this uuid to the actual key name.
        # The actual key name will be in the form <hostname>#<filename>
        # This allows the operation to be atomic.  We can either search and find the object, or we can't.  Before, there was a race condition where we could be extracting
        # the key at the exact moment of insertion.  If we dont find a key with "#" in the name of the key, remove it from results.  We shouldn't be searching against
        # objects that dont contain # in the keyname.
        temp_results = {}
        for redis_server in self._redis_corelist:
            temp_results[redis_server] = []
            for named_object in self._named_object_results[redis_server]:
                if "#" in named_object:
                    temp_results[redis_server].append(named_object)
                else:
                    if self._verbose:
                        print "(+) CacheExtractor.list_of_matching_named_objects() named_object " + named_object + " removed from redis server " + redis_server
            self._named_object_results[
                redis_server] = temp_results[redis_server]

        machines = []
        if self._range_query:
            for range_server in self._range_servers:
                if self._verbose:
                    print "(+) CacheExtractor.list_of_matching_named_objects() range_server is ", range_server
                    print "(+) CacheExtractor.list_of_matching_named_objects() self.range_query is ", self._range_query
                try:
                    range_connection = seco.range.Range(range_server)
                    machines = range_connection.expand(self._range_query)
                    if machines:
                        break
                except seco.range.RangeException:
                    print "(+) CacheExtractor.list_of_matching_named_objects() range query invalid"
                    sys.exit(1)

        if self._file:
            try:
                if machines:
                    if self._file == "-":
                        boxes = sys.stdin.readlines()
                    else:
                        boxes = open(self._file, 'r').readlines()
                    for box in boxes:
                        machines.append(box)
                else:
                    if self._file == "-":
                        machines = sys.stdin.readlines()
                    else:
                        machines = open(self._file, 'r').readlines()
            except Exception, e:
                print "The file " + self._file + " can not be opened.  Does it exist?  Exiting."
                sys.exit(1)

        # Both range queries and reading from a file are both restrictive
        # actions.  We only return objects if we match from either source.
        if self._range_query or self._file:
            temp_results = {}
            for redis_server in self._redis_corelist:
                temp_results[redis_server] = []
                for named_object in self._named_object_results[redis_server]:
                    for machine in machines:
                        if machine.strip() in named_object:
                            temp_results[redis_server].append(named_object)
                self._named_object_results[
                    redis_server] = temp_results[redis_server]

##########################################################################
    def extract_named_objects(self):

        threads = []
        for redis_server in self._redis_corelist:
            thread = threading.Thread(
                target=self.threaded_object_extractor, args=(redis_server,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Update totals for all cost hitting all redis servers.
        for redis_server in self._redis_corelist:
            self.info['totals']['total_bytes_redis_cache_downloaded'] += self.info[
                'redis_servers'][redis_server][self._uuid]['bytes_redis_cache_downloaded']
            self.info['totals']['total_bytes_results_decompressed'] += self.info[
                'redis_servers'][redis_server][self._uuid]['bytes_results_decompressed']
            self.info['totals'][
                'total_keys_matched'] += self.info['redis_servers'][redis_server][self._uuid]['keys_matched']

        import time
        self.info['totals']['total_time_finished'] = time.time()
        self.info['totals']['total_time_elapsed'] = self.info['totals'][
            'total_time_finished'] - self.info['totals']['total_time_start']
        self.info['totals']['total_bytes_per_second'] = int(self.info['totals'][
                                                            'total_bytes_redis_cache_downloaded'] / self.info['totals']['total_time_elapsed'])
        self.info['totals']['total_human_readable_mb_redis_cache_downloaded'] = '%.3f' % (
            self.info['totals']['total_bytes_redis_cache_downloaded'] / 1048576.0)
        self.info['totals']['total_human_readable_mb_results_decompressed'] = '%.3f' % (
            self.info['totals']['total_bytes_results_decompressed'] / 1048576.0)
        self.info['totals']['total_human_readable_mb_per_second'] = '%.3f' % (
            self.info['totals']['total_bytes_per_second'] / 1048576.0)
        if self.info['totals']['total_bytes_redis_cache_downloaded'] != 0:
            self.info['totals']['total_compression_ratio'] = '%.3f' % (float(self.info['totals'][
                                                                       'total_bytes_results_decompressed']) / float(self.info['totals']['total_bytes_redis_cache_downloaded']))

##########################################################################
    def threaded_object_extractor(self, redis_server):

        redis_connection = redis.Redis(
            host=redis_server, port=6379, db=self._database, socket_timeout=5, charset='utf-8', errors='strict')
        redis_pipeline = redis_connection.pipeline()
        import time
        self.info['redis_servers'][redis_server] = {}
        self.info['redis_servers'][redis_server][self._uuid] = {}
        self.info['redis_servers'][redis_server][
            self._uuid]['redis_server'] = redis_server
        self.info['redis_servers'][redis_server][
            self._uuid]['time_start'] = time.time()
        self.info['redis_servers'][redis_server][self._uuid][
            'keys_matched'] = len(self._named_object_results[redis_server])
        self._object_store[redis_server] = []

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
                elif self._time:
                    redis_pipeline.lindex(named_object, self._index_time)

            # This lies outside the loop of named_objects.  The pipeline.execute() below will issue a single redis query to fetch everything at once.
            # By using pipelines instead of individual fetches, this reduces cross communcation between the client and server.  See here for more details.
            # https://github.com/andymccurdy/redis-py
            self._object_store[redis_server] = redis_pipeline.execute()
        except redis.exceptions.ResponseError, e:
            print "CacheExtractor.threaded_object_extractor() Exception " + str(e)
            sys.exit(1)

        # The named objects array and object store arrays should be a 1-to-1 mapping at this point.  Iterate over both arrays in parallel.
        # named_object_results[redis_server] = names of the keys
        # object_store[redis_server] = whatever we extracted from the redis server for all of the keys
        # gold[name of key] = whatever we extracted
        self.info['redis_servers'][redis_server][
            self._uuid]['bytes_redis_cache_downloaded'] = 0
        self.info['redis_servers'][redis_server][
            self._uuid]['bytes_results_decompressed'] = 0
        uniques = {}

        while self._named_object_results[redis_server]:
            named_object = self._named_object_results[redis_server].pop()
            host, file = named_object.split('#')
            if self._object_store[redis_server]:
                # We are in data extraction mode with contents, md5sum, stat,
                # wordcount, or time
                contents_of_named_object = self._object_store[
                    redis_server].pop()
                if contents_of_named_object:
                    decompressed_data = bz2.decompress(
                        contents_of_named_object)
                    self.info['redis_servers'][redis_server][self._uuid][
                        'bytes_redis_cache_downloaded'] += sys.getsizeof(contents_of_named_object)
                    self.info['redis_servers'][redis_server][self._uuid][
                        'bytes_results_decompressed'] += sys.getsizeof(decompressed_data)
                    if self._return_randomized_servers == "True":
                        self._gold[named_object] = decompressed_data
                    else:
                        self._gold[
                            named_object + "@" + redis_server] = decompressed_data
            else:
                # We are either in --search or --list-files operations.  We didn't actually extract data.  if we are in --list-files, we want a list of unique
                # objects, so we build a dictionary to perform the uniques for
                # us.
                if self._list_files:
                    uniques[file] = 1
                else:
                    # We are searching, so we want non-unique filename objects
                    # across multiple hosts
                    if self._return_randomized_servers == "True":
                        self._gold[named_object] = None
                    else:
                        self._gold[named_object + "@" + redis_server] = None

            if self._verbose:
                print "(+) CacheExtractor.threaded_object_extractor() file " + file + " from host " + host + " discovered from redis server " + redis_server

        if self._list_files:
            for file in uniques.iterkeys():
                self._gold['files#' + file] = None

        # Update per-redis server information and publish to each redis server its own statistics
        # Calculate data transfer in bytes
        self.info['redis_servers'][redis_server][
            self._uuid]['time_finished'] = time.time()
        self.info['redis_servers'][redis_server][self._uuid]['time_elapsed'] = self.info['redis_servers'][
            redis_server][self._uuid]['time_finished'] - self.info['redis_servers'][redis_server][self._uuid]['time_start']
        self.info['redis_servers'][redis_server][self._uuid]['bytes_per_second'] = int(self.info['redis_servers'][redis_server][
                                                                                       self._uuid]['bytes_redis_cache_downloaded'] / self.info['redis_servers'][redis_server][self._uuid]['time_elapsed'])
        if self.info['redis_servers'][redis_server][self._uuid]['bytes_redis_cache_downloaded'] != 0:
            self.info['redis_servers'][redis_server][self._uuid]['compression_ratio'] = '%.3f' % (float(self.info['redis_servers'][redis_server][
                                                                                                  self._uuid]['bytes_results_decompressed']) / float(self.info['redis_servers'][redis_server][self._uuid]['bytes_redis_cache_downloaded']))

        # Convert to megabytes so its somewhat more human readable.
        self.info['redis_servers'][redis_server][self._uuid]['human_readable_mb_redis_cache_downloaded'] = '%.3f' % (
            self.info['redis_servers'][redis_server][self._uuid]['bytes_redis_cache_downloaded'] / 1048576.0)
        self.info['redis_servers'][redis_server][self._uuid]['human_readable_mb_results_decompressed'] = '%.3f' % (
            self.info['redis_servers'][redis_server][self._uuid]['bytes_results_decompressed'] / 1048576.0)
        self.info['redis_servers'][redis_server][self._uuid]['human_readable_mb_per_second'] = '%.3f' % (
            self.info['redis_servers'][redis_server][self._uuid]['bytes_per_second'] / 1048576.0)

        # Append the global query information
        self.info['redis_servers'][redis_server][
            self._uuid]['query'] = self.info['query']
        # Send to sysops-api
        redis_connection.publish(
            'sysops-api', json.dumps(self.info['redis_servers'][redis_server]))

##########################################################################
