#!/usr/bin/python2.6
# filesource    \$HeadURL: svn+ssh://csvn@esv4-sysops-svn.corp.linkedin.com/export/content/sysops-svn/cfengine/trunk/generic_cf-agent_policies/config-general/manage_usr_local_admin/RedisFinder.py $
# version       \$Revision: 122817 $
# modifiedby    \$LastChangedBy: msvoboda $
# lastmodified  \$Date: 2014-06-11 10:02:33 -0400 (Wed, 11 Jun 2014) $

# (c) [2013] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import os
import sys
from random import randrange
import platform
import site
import redis
import time
import signal
if platform.machine() == "sun4v" or platform.machine() == "sun4u":
    site.addsitedir('/export/apps/amfstats/lib/python2.6/site-packages')
else:
    site.addsitedir('/usr/local/linkedin/lib/python2.6/site-packages')
import seco.range

##########################################################################


class timeout_exception(Exception):
    pass

##########################################################################


class RedisFinder():

    """
    scope can be set to 3 different levels.
    local (default) = query only the MPS local to this machine (primary mps) -- this should already be balanced via select_class in cfe.
    site = query all cores for a given datacenter.  some datacenters will only have one core.  other sites can have 10x cores.
    global = query all cores at all sites (expensive)
    """

    def __init__(self, verbose=0, scope='local', site=None, range_servers=None, return_randomized_servers=True):
        if scope:
            self._scope = scope
        else:
            self._scope = 'local'
        self._redis_corelist = []
        self._site = site
        self._return_randomized_servers = return_randomized_servers
        self._cm_conf = open('/etc/cm.conf', 'r')
        self._range_servers = []
        if range_servers is not None:
            for rs in range_servers.split(','):
                self._range_servers.append(rs)
        self._verbose = verbose
        if self._verbose:
            print "(+) RedisFinder __init__ scope", self._scope
            print "(+) RedisFinder __init__  redis_corelist", self._redis_corelist
            print "(+) RedisFinder __init__  site", self._site
            print "(+) RedisFinder __init__  cm.conf", self._cm_conf
            print "(+) RedisFinder __init__  range_servers", self._range_servers

    def timeout_handler(signum, frame):
        raise timeout_exception()

    def get_verbose(self):
        return self._verbose

    def set_verbose(self, verbose):
        self._verbose = verbose
        if self._verbose:
            print "(+) RedisFinder.set_verbose() is being set to", self._verbose

    def get_scope(self):
        return self._scope

    def set_scope(self, scope):
        self._scope = scope
        if self._verbose:
            print "(+) RedisFinder.set_scope() is being set to", self._scope

    def get_return_randomized_servers(self):
        return self._return_randomized_servers

    def set_return_randomized_servers(self, return_randomized_servers):
        self._return_randomized_servers = return_randomized_servers
        if self._verbose:
            print "(+) RedisFinder.set_return_randomized_servers() is being set to", return_randomized_servers

    def get_site(self):
        return self._site

    def set_site(self, site):
        self._site = site
        if self._verbose:
            print "(+) RedisFinder.set_site() is being set to", site

    def discover_site(self):
        self._cm_conf.seek(0)
        for line in self._cm_conf.readlines():
            if "ENV_SITE" in line:
                self._site = line.split('@')[1].lower().rstrip()
                if self._verbose:
                    print "(+) RedisFinder.discover_site() is being set to", self._site

    def get_range_servers(self):
        return(self._range_servers)

    def set_range_servers(self, range_servers):
        self._range_servers = range_servers
        if self._verbose:
            print "(+) RedisFinder.set_range_servers() is being set to", self._range_servers

    def discover_range_servers(self):
        self._cm_conf.seek(0)
        for line in self._cm_conf.readlines():
            if "MPS" in line:
                self._range_servers.append(line.split(':')[1].rstrip())
        if self._verbose:
            print "(+) RedisFinder.discover_range_servers() is being set to", self._range_servers

    def get_redis_corelist(self):
        return(self._redis_corelist)

    def set_redis_corelist(self, redis_corelist):
        self._redis_corelist = redis_corelist
        if self._verbose:
            print "(+) RedisFinder.set_redis_corelist() redis_corelist being set to", slef._redis_corelist

    def query_range_for_redis_corelist(self):
        """
        If we make a successful range query, then break out of the loop as there is no point to query range multiple times.
        We provide the loop here so if the first range server is down, hopefully the latter will succeed.
        If "site" isn't provided, but the scope is set to site, we assume the local datacenter that the query is occuring from.

        A single deference to *-mps-cores returns the cores for that site.  A double deference to *-mps-cores returns the actual redis
        servers for that site.  i.e.
        msvoboda-mn:trunk msvoboda$ eh -e %cf3.promises.esv4-mps-cores
        cf3.promises.esv4-2360-mps

        msvoboda-mn:trunk msvoboda$ eh -e %%cf3.promises.esv4-mps-cores
        esv4-2360-mps01.corp.linkedin.com
        esv4-2360-mps02.corp.linkedin.com
        esv4-cfe-test.corp.linkedin.com
        """

        if not self._range_servers:
            self.discover_range_servers()

        if self._scope == "local":
            # return the primary mps, which should already be load balanced via
            # select_class generated by Cfengine.
            self._redis_corelist.append(self._range_servers[0])
            if self._verbose:
                print "(+) RedisFinder.query_range_for_redis_corelist() setting redis_corelist to", self._redis_corelist
        elif self._scope == "site":
            if not self._site:
                self.discover_site()
            else:
                if self._verbose:
                    print "(+) RedisFinder.query_range_for_redis_corelist() self._site is set to", self._site
            range_query = "%cf3.promises." + self._site + "-mps-cores"
            if self._verbose:
                print "(+) RedisFinder.query_range_for_redis_corelist() range_query is set to", range_query
        elif self._scope == "global":
            range_query = "%cf3.promises.all-mps-cores"
            if self._verbose:
                print "(+) RedisFinder.query_range_for_redis_corelist() range_query is set to", range_query
        else:
            # slef._scope is set to something undefined.  set the corelist to
            # None and break out.
            self._redis_corelist = None
            if self._verbose:
                print "(+) RedisFinder.query_range_for_redis_corelist() undefined scope", self._scope
            sys.exit(1)

        if self._scope != "local":
            total_redis_corelist = None
            while not total_redis_corelist:
                for range_server in self._range_servers:
                    try:
                        range_connection = seco.range.Range(range_server)
                        if self._verbose:
                            print "(+) RedisFinder.query_range_for_redis_corelist() making a range connection to", range_server
                        total_redis_corelist = range_connection.expand(
                            range_query)
                        if total_redis_corelist:
                            if self._verbose:
                                print "(+) RedisFinder.query_range_for_redis_corelist() total redis corelist is", total_redis_corelist
                            # Since we've discovered our corelist, stop cycling
                            # through avilable range servers.
                            break
                        else:
                            if self._verbose:
                                print "(+) RedisFinder.query_range_for_redis_corelist() no redis corelist was returned from range server", range_server
                    except seco.range.RangeException:
                        self._redis_corelist = None
                        if self._verbose:
                            print "(+) RedisFinder.query_range_for_redis_corelist() range exception returned from range server", range_server
                            print "(+) RedisFinder.query_range_for_redis_corelist() attempting to query the next range server"
                    if not total_redis_corelist:
                        # if we get to this point, we were unable to populate
                        # the redis corelist.  Exit out.
                        sys.exit(1)

                # Now, for each corelist, return a randomized redis server from
                # that list and append to self._redis_corelist.
                for single_redis_core in total_redis_corelist:
                    redis_servers_single_core = range_connection.expand(
                        "%" + single_redis_core)
                    if self._verbose:
                        print "(+) RedisFinder.query_range_for_redis_corelist() redis servers single core is ", redis_servers_single_core

                    if not self._return_randomized_servers:
                        for single_redis_server in redis_servers_single_core:
                            self._redis_corelist.append(single_redis_server)
                            if self._verbose:
                                print"(+) RedisFinder.query_range_for_redis_corelist() pushed redis server is ", self._redis_corelist
                    else:
                        while redis_servers_single_core:
                            corelist_valid = False
                            random_redis_server_index = randrange(
                                0, len(redis_servers_single_core))
                            if self._verbose:
                                print "(+) RedisFinder.query_range_for_redis_corelist() random_redis_server_index is ", random_redis_server_index
                            redis_server = redis_servers_single_core.pop(
                                random_redis_server_index)
                            try:
                                redis_connection = redis.Redis(
                                    host=redis_server, port=6379, db=0, socket_timeout=1, charset='utf-8', errors='strict')
                                redis_info = redis_connection.info()
                                if redis_info:
                                    self._redis_corelist.append(redis_server)
                                    if self._verbose:
                                        print"(+) RedisFinder.query_range_for_redis_corelist() pushed redis server is ", self._redis_corelist
                                    # If we made a successful query and got a redis server for the core, there is no need to attempt to cycle through the others.
                                    # Break out of here and move onto the next
                                    # core to find the next redis server to
                                    # push.
                                    corelist_valid = True
                                    break
                            except redis.exceptions.ConnectionError:
                                continue

                        if not corelist_valid:
                            print "(+) RedisFinder.query_range_for_redis_corelist() All redis servers in " + single_redis_core + " were unresponsive.  Its possible that you are being firewalled by Network ACLs.  Please attempt to run this utility from a production host, which should have network connectivity to all the Redis servers. This utility is exiting, as the results would be from an incomplete dataset."
                            sys.exit(1)
##########################################################################


def main():
    print "local scope"
    redisResults = RedisFinder(verbose=1, scope='local')
    redisResults.query_range_for_redis_corelist()

    print "site scope"
    redisResults = RedisFinder(verbose=1, scope='site')
    redisResults.query_range_for_redis_corelist()

    print "global scope"
    redisResults = RedisFinder(verbose=1, scope='global')
    redisResults.query_range_for_redis_corelist()
##########################################################################
if __name__ == '__main__':
    main()
