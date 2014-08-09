#!/usr/bin/python2.6

# (c) [2013] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import sys
sys.path.append("/usr/local/admin")
sys.path.append("/usr/local/linkedin/lib/python2.6/site-packages")
import sysopsapi.redis_finder
import os
import seco.range
import sysopsapi.cache_extractor
import time
import shutil
import signal
import gc
import datetime


class timeout_exception(Exception):
    pass


def timeout_handler(signum, frame):
    raise timeout_exception()

##########################################################################


def execute_range_query():
    range_query = "%cf3.promises.all-mps-cores"
    range_servers = []
    cm_conf = open("/etc/cm.conf", 'r')
    for line in cm_conf.readlines():
        if "MPS" in line:
            range_servers.append(line.split(':')[1].rstrip())

    while range_servers:
        try:
            range_server = range_servers.pop()
            range_connection = seco.range.Range(range_server)
            total_redis_corelist = range_connection.expand(range_query)
            if total_redis_corelist:
                break
        except:
            pass
    return(total_redis_corelist)
##########################################################################


def main():
    base_directory = "/mnt/u001/sysops-api/"

    start_time = int(time.time())
    print "Starting Cycle at " + time.ctime()
    sites = {}
    global total_redis_corelist
    total_redis_corelist = execute_range_query()

    for core in total_redis_corelist:
        dc = core.split('.')[2].split('-')[0]
        sites[dc.upper()] = 1

    files_written = {}
    for datacenter in sites.iterkeys():
        start_dc_time = time.time()
        if not os.path.exists(base_directory + datacenter):
            os.makedirs(base_directory + datacenter)

        # Find the unique files for the datacenter.  Spawn a sysopsapi.cache_extractor
        # object for every key.  Dumping the entire datacenter in a single
        # object requires too much RAM.
        uniqueFiles = sysopsapi.cache_extractor.CacheExtractor(
            scope='site', site=datacenter.lower(), list_files=True)

        number_of_keys = len(uniqueFiles._gold.keys())
        keys_processed = 0
        for key in sorted(uniqueFiles._gold.iterkeys()):
            keys_processed += 1
            start_file_time = time.time()
            filename = key.split("#")[1]
            # Spawn a sysopsapi.cache_extractor object for every unique key, in every
            # datacenter.
            try:
                dataDump = sysopsapi.cache_extractor.CacheExtractor(
                    scope='site', site=datacenter.lower(), search_string=filename, contents=True)
            except Exception, e:
                print "Lost filename " + filename + " The object has dropped out of the cache"
                continue

            for key in sorted(dataDump._gold.iterkeys()):
                host, file = key.split('#')
                hostdir = base_directory + datacenter + "/" + host
                if not os.path.exists(hostdir + os.path.dirname(file)):
                    os.makedirs(hostdir + os.path.dirname(file))
                if dataDump._gold[key]:
                    tmp_target_file_name = hostdir + \
                        os.path.dirname(file) + "/." + os.path.basename(file)
                    target_file_name = hostdir + file
                    tmp_target_file = open(tmp_target_file_name, 'w')
                    tmp_target_file.write(dataDump._gold[key])
                    tmp_target_file.close()
                    shutil.move(tmp_target_file_name, target_file_name)
                    files_written[target_file_name] = 1
            del dataDump
            gc.collect()
            print "Completed filename " + filename + " in datacenter " + datacenter + " in " + str(int(time.time() - start_file_time)) + " seconds. " + str(number_of_keys - keys_processed) + " keys remain for the datacenter."
            print "Total time elapsed is " + str(datetime.timedelta(seconds=int(time.time() - start_time)))

        del uniqueFiles
        gc.collect()
        print "Completed datacenter " + datacenter + " in " + str(int(time.time() - start_dc_time)) + " seconds"

    purge_start = time.time()
    print "Starting purge of stale data at " + time.ctime()
    files_on_disk = {}
    for (path, dirs, files) in os.walk(base_directory):
        for file in files:
            files_on_disk[path + "/" + file] = 1

    for file in files_on_disk.iterkeys():
        if not files_written.get(file):
            print "removing stale file " + file
            os.remove(file)
    print "Purge of stale data completed in " + str(int(time.time() - purge_start)) + " seconds"

    print "Completed execution in " + str(int(time.time() - start_time)) + " seconds"
    # 20 minutes * 60 seconds = 1200
    seconds_left = 1200 - int(time.time() - start_time)
    print "Sleeping for " + str(seconds_left) + " seconds"
    if seconds_left > 0:
        time.sleep(seconds_left)

if __name__ == '__main__':

    # Set a 60 minute alarm
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    # esv4-infra07 takes a long time to purge stale data.  Set this timeout
    # very high until something else can be figured out.
    signal.alarm(21600)
    "(+) Starting 6 hour signal alarm"
    try:
        main()
    except timeout_exception:
        print "6 hours exceeded.  Bailing"
    finally:
        signal.signal(signal.SIGALRM, old_handler)
        signal.alarm(0)
