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

    ina_groups = []
    ina_group_file = open(
        '/etc/sudo.d/sudoers-USERS_GROUP_WORLD_READABLE', 'r').readlines()
    for line in ina_group_file:
        if options.user in line:
            ina_groups.append(line.split()[1])

    sudoers_rules = {}
    sudoers_file = open('/etc/sudo.d/sudoers_WORLD_READABLE', 'r').readlines()
    for line in sudoers_file:
        if " = " in line:
            ina_group = line.split()[0].strip()
            machine_group = line.split()[1].strip()
            privs = line.split('=')[1].strip()

            if ina_group in ina_groups:
                sudoers_rules[machine_group] = privs

    cache_results = sysopsapi.cache_extractor.CacheExtractor(verbose=options.verbose,
                                                  scope='global',
                                                  contents=True,
                                                  range_query=options.range_query,
                                                  search_string='sudoers-MACHINE_GROUP')

    for key in cache_results._gold.iterkeys():
        host = key.split('#')[0]
        for line in cache_results._gold[key].splitlines():
            if "Host_Alias" in line:
                system_machine_group = line.split()[1]
                if sudoers_rules.get(system_machine_group):
                    print ("user: " + options.user).ljust(0) + \
                          ("privs: " + sudoers_rules.get(system_machine_group)).center(40) + \
                          ("host: " + host).center(40) + \
                          ("machine_group: " + system_machine_group).rjust(10)

##########################################################################
if __name__ == '__main__':
    main()
