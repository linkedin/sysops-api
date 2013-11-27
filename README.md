sysops-api
===============================

© [2013] LinkedIn Corp. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

Operational Visibility
===============================
sysops-api is a framework that leverages Redis to provide visibility from tens of thousands of machines in seconds.  Instead of trying to SSH to remote machines to collect data (execute commands, grep through files), LinkedIn uses this framework to answer any arbitrary question about all of our infrastructure.

*[The slides from the  LISA 2013 presentation which describes the architecture can be found here](http://www.slideshare.net/MikeSvoboda/lisa-2013-sysopsapi-leveraging-inmemory-key-value-stores-for-large-scale-operations-with-redis-and-cfengine)*

*[Video from LISA 2013 of this presentation can be watched here](http://youtu.be/H1dVsSvKBlM)*


This project is basically a means for us to answer any arbitrary question about any production machine and get results returned to us in seconds.   We primarily use this for crawling tens of thousands of machines for information very quickly.   Its how we can confidently make automation changes.  We use this to audit production before a change is pushed, so we know the state of machines and what our change will impact.  

    [msvoboda@esv4-infra01 ~]$ time extract_sysops_cache.py --search sysctl-a --contents --scope global | grep net.core.netdev_budget  | uniq -c | sort -n
    24247 net.core.netdev_budget = 300

    real	0m28.337s
    user	0m45.244s
    sys  	0m2.837s

The above command searched the output of "sysctl –a" across 24 thousand machines in 28 seconds and reported that all machines had the same value for net.core.netdev_budget = 300.   This means if we wanted to use automation to control this kernel tunable, we would not impact any systems.

What kind of data is this fetching?
===============================
We use this to insert any type of information we want.  This could be remotely executed commands (netstat, installed packages, loaded kernel modules, etc) or any file off of the filesystem.     We take snapshots of our systems, and use these snapshots to answer any question we want.   Very fast.  

Insert process tables, mount tables, loaded kernel modules, /proc things, or whatever else you use on an every day basis as a sysadmin to debug systems.  Once this data exists in sysops-api, you will find that you no longer have to log in to machines remotely again to debug them.  Provide as much data as possible up front so you can answer any question thrown at you.

Cache Insertion
===============================
At a high level, we create a directory and symlink arbitrary objects into it.  We use CFEngine to populate Redis, so we've chosen /var/cfengine/outgoing,
but the directory could be anywhere.  The provided python script, module_populate_mps_cache, searches for all files in this directory and inserts them
into Redis.  If we want to collect remotely executed commands, we do so externally from this tool and dump them into a JSON object called
/etc/hardware_identification.json.  On insertion, we populate metadata about the object (md5sum, os.stat, wordcount) and insert at the same time. 
Optionally, we also record the amount of time required for each Redis server interaction.   Each machine at LinkedIn sends data to 4x Redis servers.  The
Redis servers themselves do not replicate data.  Replication is handled by the clients at data insertion time.   The data insertion script assumes that
4x Redis servers are passed on the CLI.  Modify this if you want.  If we only want to send data to 2x Redis servers, we duplicate physical machine names, but still supply 4x.  This allows us to use the same CFEngine policy to insert data into 1, 2, or 4x Redis servers without code modification.

We use CFEngine to drive execution of module_populate_mps_cache, but there is no reason it couldn't be driven via crontab.  All that happens in this
script is that it finds objects to send into the cache, and does that.

Cache Extraction
===============================
For data extraction, The following steps are required:

 Provided by RedisFinder.py:
  1. Determine scope (local, site, or global)
  2. For each level of my scope, determine what Redis servers I will contact.
  3. Pick a randomized Redis server from this list (load balancing)
  4. If the randomly chosen Redis server doesn't respond, pick another one at random (failover)
  5. Test that a Redis server.info() call works.  If it does, push it into the list of Redis servers that we should querty to build our working set.

LinkedIn uses a technology opensourced by Yahoo! called Range to perform this lookup.  https://github.com/ytoolshed/range

The method of lookup could be:
 DNS txt records
 Files on the filesystem
 LDAP
 Any other directory service

Please add any other lookup services into RedisFinder.py as needed.


 Provided by CacheExtractor.py:
  1. Search each Redis server for matching keys based off of the search string passed to the CacheExtractor object.
  2. (Optionally) Extract data from the matched keys. 

The cache exists as a simple python dictionary (hash). The key is in the format of
<hostname>#<filename>

The value of the key is an array.  
    Array[0] = Contents of the file / command 
    Array[1] = md5sum
    Array[2] = Python's os.stat()
    Array[3] =  wordcount

CacheExtractor
===============================
The basic interaction with sysops-api is the CacheExtractor python object.  The "swiss army tool" called extract_sysops_cache.py imports this object,
makes the output pretty, and displays to standard out.  Typically this is how users interact with the CacheExtractor object from the command line. 

CacheExtractor gives you a programmatic method of interacting with the results pulled out of the cache.  For example, its trival to search for 8000
JSON objects, (lshw --output json) and access the hardware characteristics of an entire datacenter by programatically.  Several other utilities are included in the contrib section showing how to use this module. 

More Documentation
===============================
*[Usage of extract_sysops_cache.py](https://github.com/linkedin/sysops-api/wiki/Extracting-the-Sysops-cache-for-fun-and-profit)*

