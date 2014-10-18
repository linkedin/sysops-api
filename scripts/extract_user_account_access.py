#!/usr/bin/python2.6

# (c) [2013] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

from optparse import OptionParser
import sys
sys.path.append("/usr/local/admin")
import sysopsapi.cache_extractor

##########################################################################


def main():
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")
    parser.add_option("--verbose",
                      action="store_true",
                      dest="verbose",
                      default=False,
                      help="Enable verbose execution")
    parser.add_option("--range-query",
                      action="store",
                      dest="range_query",
                      help="Specify a range cluster of hosts you which to use to use to make queries against.")
    parser.add_option("--user",
                      action="store",
                      dest="user",
                      help="Specify a unix user uid or id that you are interested in searching for.")

    (options, args) = parser.parse_args()

    cache_results = sysopsapi.cache_extractor.CacheExtractor(verbose=options.verbose,
                                                  scope='global',
                                                  contents=True,
                                                  range_query=options.range_query,
                                                  search_string='/etc/passwd',)

    accessable_machines = {}

    for key in cache_results._gold.iterkeys():
        host = key.split('#')[0]
        accessable_machines[host] = None
        for line in cache_results._gold[key].splitlines():
            if options.user in line:
                accessable_machines[host] = 1

    print "User " + options.user + " has access to the following machines:"
    for machine in sorted(accessable_machines.iterkeys()):
        if accessable_machines[machine]:
            print machine

    print "*******************************************".center(100)
    print "*******************************************".center(100)
    print "*******************************************".center(100)
    print "*******************************************".center(100)

    print "User " + options.user + " does not have access to the following machines:"
    for machine in sorted(accessable_machines.iterkeys()):
        if not accessable_machines[machine]:
            print machine
##########################################################################
if __name__ == '__main__':
    main()
