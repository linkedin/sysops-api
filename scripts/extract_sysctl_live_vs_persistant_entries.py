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
import re

##########################################################################


def main():
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")
    parser.add_option("--verbose",
                      action="store_true",
                      dest="verbose",
                      default=False,
                      help="Enable verbose execution")
    parser.add_option("--site",
                      action="store",
                      dest="site",
                      help="Specify a datacenter to query against.")

    (options, args) = parser.parse_args()

    if options.site:
        myscope = 'site'
    else:
        myscope = 'global'

    persistant_results = sysopsapi.cache_extractor.CacheExtractor(verbose=options.verbose,
                                                       scope=myscope,
                                                       contents=True,
                                                       site=options.site,
                                                       search_string='/etc/sysctl.conf')

    for key in persistant_results._gold.iterkeys():
        host = key.split('#')[0]
        values = {}
        for line in persistant_results._gold[key].splitlines():
            if "#" not in line and line != "":
                line = re.sub("\s+", " ", line)
                kernel_tunable = line.split()[0]
                if kernel_tunable != "vm.dirty_writeback_centisecs" and kernel_tunable != "vm.dirty_expire_centisecs" and kernel_tunable != "kernel.core_pattern":
                    values[kernel_tunable] = line

        while True:
            try:
                live_results = sysopsapi.cache_extractor.CacheExtractor(verbose=options.verbose,
                                                             scope=myscope,
                                                             contents=True,
                                                             site=options.site,
                                                             search_string=host + "#/etc/hardware_identification.json@sysctl-a")
            except Exception, e:
                continue
            break

        for key in live_results._gold.iterkeys():
            host = key.split('#')[0]
            for line in live_results._gold[key].splitlines():
                line = re.sub("\s+", " ", line)
                kernel_tunable = line.split()[0]
                if values.get(kernel_tunable):
                    if values[kernel_tunable] != line:
                        print "found difference on " + host + " at kernel tunable " + kernel_tunable
                        print host + " persistant value " + values[kernel_tunable]
                        print host + " live value " + line
                        print
        print "Completed host " + host


##########################################################################
if __name__ == '__main__':
    main()
