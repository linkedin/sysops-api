#!/usr/bin/python2.6
# filesource    \$HeadURL: svn+ssh://csvn@esv4-sysops-svn.corp.linkedin.com/export/content/sysops-svn/cfengine/trunk/generic_cf-agent_policies/config-general/manage_usr_local_utilities/extract_network_health.py $
# version       \$Revision: 78161 $
# modifiedby    \$LastChangedBy: msvoboda $
# lastmodified  \$Date: 2013-12-03 16:30:07 -0500 (Tue, 03 Dec 2013) $

# (c) [2013] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

from optparse import OptionParser
import sys
import os
sys.path.append("/usr/local/admin")
import sysopsapi.cache_extractor
import json
import numpy as np
import time
from xml.dom import minidom
import seco.range
import re
import datetime
import shutil
import uuid
import bz2

##########################################################################


def validate_json_data(host_stats, site, local_disk, workdir):
    site = site[0]
    if not local_disk:
        cache_results = sysopsapi.cache_extractor.CacheExtractor(
            contents=True, scope='site', search_string=site + '-host_stats.json')
        try:
            if cache_results:
                # This should only be returning a single site, but we need to
                # get access to the key.
                for key in cache_results._gold.iterkeys():
                    host_stats = json.loads(
                        bz2.decompress(cache_results._gold[key]))
        except ValueError:
            pass

    if local_disk:
        # If len(host_stats) == 5, then we didn't populate the JSON data.  We need to read from disk.
        # Length is 5 because we initialize host_stats at the start of
        # generate_json_data.
        if len(host_stats) == 5:
            try:
                with open(workdir + '/' + site + '-host_stats.json', 'r') as fh:
                    data = fh.read()
                    host_stats = json.loads(bz2.decompress(data))
            except Exception, e:
                print "Could not read data from site " + site + str(e)
                print "Generating local disk data now"
                generate_json_data(site.split(), workdir, refresh=True,)
                sys.exit(1)

    # figure out whats causing this
    for datastructure in ['per_redis_server_interaction_time', 'per_redis_server_transfer_speed']:
        for calc in ['mean', 'median', 'var']:
            del host_stats[datastructure][calc]

    # Force all data to be float, not unicode
    for datastructure in ['total_redis_interaction_time', 'total_redis_transfer_speed']:
        for calc in ['mean', 'median', 'var']:
            host_stats[datastructure][calc] = map(
                float, host_stats[datastructure][calc])

    for datastructure in ['per_redis_server_transfer_speed', 'per_redis_server_interaction_time']:
        for redis_server in host_stats[datastructure]:
            host_stats[datastructure][redis_server]['median'] = map(
                float, host_stats[datastructure][redis_server]['median'])
            host_stats[datastructure][redis_server]['mean'] = map(
                float, host_stats[datastructure][redis_server]['mean'])

    return host_stats
##########################################################################


def generate_json_data(sites, workdir, refresh):

    for site in sites:
        host_stats = {}
        host_stats['machines'] = {}
        for datastructure in ['total_redis_interaction_time', 'total_redis_transfer_speed', 'per_redis_server_interaction_time', 'per_redis_server_transfer_speed']:
            host_stats[datastructure] = {}
            host_stats[datastructure]['median'] = []
            host_stats[datastructure]['mean'] = []
            host_stats[datastructure]['var'] = []

        # Delete the data and rebuild if its older than 60 minutes or if
        # --refresh was passed on the CLI
        if os.path.exists(workdir + '/' + site + '-host_stats.json'):
            if refresh or (global_start_time - os.stat(workdir + '/' + site + '-host_stats.json').st_mtime > 3600):
                os.remove(workdir + '/' + site + '-host_stats.json')

        # Generate the JSON data if it doesn't exist
        if not os.path.exists(workdir + '/' + site + '-host_stats.json'):
            host_stats = calculate_means_and_medians(site, host_stats)
            host_stats = find_active_interface_in_bond(site, host_stats)
            host_stats = find_lldpd_data(site, host_stats)
            host_stats = pull_netstat_summary(site, host_stats)
            host_stats['data_generation_time'] = time.time()

            # The datacenter is complete.  Dump all data and move onto the next
            # datacenter in daemon mode.
            try:
                # Write to a temporary file so data isn't inserted into
                # sysops-api until the write has completed.
                tempkey = str(uuid.uuid4())
                key = workdir + '/' + site + '-host_stats.json'
                data = json.dumps(host_stats, sort_keys=True, indent=5)
                bz2.BZ2File(tempkey, 'w').write(data)
                shutil.move(tempkey, key)
            except Exception, e:
                print "Could not write data from datacenter " + site + str(e)
                sys.exit(1)

    return host_stats
##########################################################################


def find_range_servers():
    # Set a dummy range query that we know to exist.
    range_query = "%cf3.promises.all-mps-cores"

    range_servers = []

    try:
        cm_conf = open("/etc/cm.conf", 'r')
        for line in cm_conf.readlines():
            if "MPS" in line:
                range_servers.append(line.split(':')[1].rstrip())
    except Exception, e:
        print e
        sys.exit(1)

    while range_servers:
        try:
            range_server = range_servers.pop()
            range_connection = seco.range.Range(range_server)
            results = range_connection.expand(query)
            if results:
                break
        except:
            pass
    return range_connection
##########################################################################


def build_valid_sites(range_connection):
    sites = {}
    for core in range_connection.expand('%cf3.promises.all-mps-cores'):
        sites[core.split("-")[0].split(".")[2]] = 1
    uniq_sites = []
    for key in sites.iterkeys():
        uniq_sites.append(key)
    return(uniq_sites)
##########################################################################


def build_valid_netstat_keys(host_stats):
    # find all unique netstat keys
    keys = []
    all_netstat_keys = {}
    for host in host_stats['machines'].iterkeys():
        for netstat_key in host_stats['machines'][host]['active_interface']['netstat_summary'].iterkeys():
            if not all_netstat_keys.get(netstat_key):
                all_netstat_keys[netstat_key] = 1
    for key in all_netstat_keys.iterkeys():
        keys.append(key)
    return(keys)
##########################################################################


def calculate_means_and_medians(datacenter, host_stats):
    start_time = time.time()
    cache_results = sysopsapi.cache_extractor.CacheExtractor(contents=True,
                                                  site=datacenter,
                                                  search_string='global_sysops_api_interaction_times.json')
    for key in cache_results._gold.iterkeys():
        host = key.split('#')[0]
        if "nw.linkedin.com" in host:
            continue
        data = json.loads(cache_results._gold[key])
        host_stats['machines'][host] = {}

        # Compute global statistics per host
        for datastructure in ['total_redis_interaction_time', 'total_redis_transfer_speed']:
            if data.get('global_total'):
                if data['global_total'].get(datastructure):
                    if data['global_total'][datastructure].get('median'):
                        host_stats['machines'][host][datastructure] = {}
                        host_stats['machines'][host][
                            datastructure]['median'] = {}
                        host_stats['machines'][host][
                            datastructure]['mean'] = {}
                        host_stats['machines'][host][datastructure]['var'] = {}
                        host_stats['machines'][host][datastructure][
                            'median'] = data['global_total'][datastructure]['median']
                        host_stats['machines'][host][datastructure][
                            'mean'] = data['global_total'][datastructure]['mean']
                        host_stats['machines'][host][datastructure][
                            'var'] = data['global_total'][datastructure]['var']
                        host_stats[datastructure]['median'].append(
                            host_stats['machines'][host][datastructure]['median'])
                        host_stats[datastructure]['mean'].append(
                            host_stats['machines'][host][datastructure]['mean'])
                        host_stats[datastructure]['var'].append(
                            host_stats['machines'][host][datastructure]['var'])

        # Compute per-mps statistics per host and per-mps globally
        for datastructure in ['per_redis_server_transfer_speed', 'per_redis_server_interaction_time']:
            if data.get('per_mps'):
                if data['per_mps'].get(datastructure):
                    host_stats['machines'][host][datastructure] = {}
                    for redis_server in data['per_mps'][datastructure].iterkeys():
                        # per host initialize
                        host_stats['machines'][host][
                            datastructure][redis_server] = {}
                        # global per-mps initialize
                        if not host_stats[datastructure].get(redis_server):
                            host_stats[datastructure][redis_server] = {}
                            host_stats[datastructure][
                                redis_server]['median'] = []
                            host_stats[datastructure][
                                redis_server]['mean'] = []
                            host_stats[datastructure][redis_server]['var'] = []

                        if data['per_mps'][datastructure][redis_server].get('median'):
                            host_stats['machines'][host][datastructure][redis_server][
                                'median'] = data['per_mps'][datastructure][redis_server]['median']
                            host_stats['machines'][host][datastructure][redis_server][
                                'mean'] = data['per_mps'][datastructure][redis_server]['mean']
                            host_stats['machines'][host][datastructure][redis_server][
                                'var'] = data['per_mps'][datastructure][redis_server]['var']
                            host_stats[datastructure][redis_server]['median'].append(
                                host_stats['machines'][host][datastructure][redis_server]['median'])
                            host_stats[datastructure][redis_server]['mean'].append(
                                host_stats['machines'][host][datastructure][redis_server]['mean'])
                            host_stats[datastructure][redis_server]['var'].append(
                                host_stats['machines'][host][datastructure][redis_server]['var'])

    print str(int(time.time() - start_time)) + " seconds elapsed for hosts insertion data discovery in datacenter " + datacenter

    return host_stats
##########################################################################


def find_active_interface_in_bond(datacenter, host_stats):
    start_time = time.time()
    cache_results = sysopsapi.cache_extractor.CacheExtractor(contents=True,
                                                  site=datacenter,
                                                  search_string="/proc/net/bonding/bond0")
    for key in cache_results._gold.iterkeys():
        host = key.split("#")[0]
        if not host_stats['machines'].get(host) or ".nw.linkedin.com" in host:
            continue
        host_stats['machines'][host]['active_interface'] = {}
        for line in cache_results._gold[key].splitlines():
            if "Currently Active Slave" in line:
                host_stats['machines'][host]['active_interface'][
                    'name'] = line.split(':')[1].strip()

    bad_hosts = []
    for host in host_stats['machines'].iterkeys():
        if not host_stats['machines'][host].get('active_interface'):
            bad_hosts.append(host)
    for host in bad_hosts:
        print "Removing host " + host + " from working set as we could not discover the active interface in the bond"
        del host_stats['machines'][host]
        continue

    print str(int(time.time() - start_time)) + " seconds elapsed for primary network interface discovery in datacenter " + datacenter
    return host_stats
##########################################################################


def find_lldpd_data(datacenter, host_stats):
    start_time = time.time()
    cache_results = sysopsapi.cache_extractor.CacheExtractor(contents=True,
                                                  site=datacenter,
                                                  search_string='/etc/hardware_identification.json@lldp-xml')
    for key in cache_results._gold.iterkeys():
        host = key.split("#")[0]
        if not host_stats['machines'].get(host) or ".nw.linkedin.com" in host:
            continue
        if host_stats['machines'][host].get('active_interface'):
            try:
                xmldoc = minidom.parseString(cache_results._gold[key])
                interfaces = xmldoc.getElementsByTagName('interface')
                for interface in interfaces:
                    if interface.attributes['name'].value == host_stats['machines'][host]['active_interface']['name']:
                        chassis = interface.getElementsByTagName('chassis')[0]
                        switch_name = chassis.getElementsByTagName('name')[0]
                        host_stats['machines'][host]['active_interface'][
                            'switch_name'] = switch_name.firstChild.toxml()
                        port = interface.getElementsByTagName('port')[0]
                        port_descr = port.getElementsByTagName('descr')[0]
                        host_stats['machines'][host]['active_interface'][
                            'network_port'] = port_descr.firstChild.toxml()
            except Exception, e:
                pass

    bad_hosts = []
    for host in host_stats['machines'].iterkeys():
        if not host_stats['machines'][host]['active_interface'].get('switch_name'):
            bad_hosts.append(host)
    for host in bad_hosts:
        if not host_stats['machines'][host]['active_interface'].get('switch_name'):
            host_stats['machines'][host][
                'active_interface']['switch_name'] = "N/A"
            host_stats['machines'][host][
                'active_interface']['network_port'] = "N/A"

    print str(int(time.time() - start_time)) + " seconds elapsed for network switch discovery in datacenter " + datacenter
    return host_stats
##########################################################################


def is_number(s):
    try:
        float(s)  # for int, long and float
    except ValueError:
        return False
    return True
##########################################################################


def pull_netstat_summary(datacenter, host_stats):
    start_time = time.time()
    cache_results = sysopsapi.cache_extractor.CacheExtractor(contents=True,
                                                  site=datacenter,
                                                  search_string='/etc/hardware_identification.json@netstat-s')
    for key in cache_results._gold.iterkeys():
        host = key.split("#")[0]
        if not host_stats['machines'].get(host) or ".nw.linkedin.com" in host:
            continue
        host_stats['machines'][host][
            'active_interface']['netstat_summary'] = {}
        for line in cache_results._gold[key].splitlines():
            # Strip the colon out of Tcp:
            if re.match('^\w+:$', line):
                subsystem = re.sub('[:]', '', line).lower().strip()
            # The actual value jumps around.  We have to test each word and determine where the numbers are.
            # Use the non-number words to construct the key.
            keyname = []
            value = None
            words = line.split()
            keyname.append(subsystem)
            for word in words:
                if is_number(word):
                    value = word
                else:
                    word = re.sub('[:.]', '', word).lower()
                    word = re.sub('[-]', '_', word).strip()
                    if word == subsystem:
                        continue
                    keyname.append(word)
            if value:
                host_stats['machines'][host]['active_interface'][
                    'netstat_summary']["_".join(keyname)] = value

        # Now that we have extracted all netstat summary data, start to build
        # some statistics that aren't available out of the box.
        if (host_stats['machines'][host]['active_interface']['netstat_summary'].get('tcp_segments_retransmited') and
                host_stats['machines'][host]['active_interface']['netstat_summary'].get('tcp_segments_send_out')):
            host_stats['machines'][host]['active_interface']['netstat_summary']['tcp_segments_retransmited_ratio'] = float(float(host_stats['machines'][host]['active_interface'][
                                                                                                                           'netstat_summary']['tcp_segments_retransmited']) / float(host_stats['machines'][host]['active_interface']['netstat_summary']['tcp_segments_send_out']))

        if (host_stats['machines'][host]['active_interface']['netstat_summary'].get('udp_packet_receive_errors') and
                host_stats['machines'][host]['active_interface']['netstat_summary'].get('udp_packets_received')):
            host_stats['machines'][host]['active_interface']['netstat_summary']['udp_packet_recieve_errors_ratio'] = float(float(host_stats['machines'][host]['active_interface'][
                                                                                                                           'netstat_summary']['udp_packet_receive_errors']) / float(host_stats['machines'][host]['active_interface']['netstat_summary']['udp_packets_received']))

    print str(int(time.time() - start_time)) + " seconds elapsed for netstat parsing in datacenter " + datacenter
    return host_stats
##########################################################################
######################################## End daemon data discovery #######
##########################################################################

##########################################################################
########################################## Start data reporting ##########
##########################################################################


def print_basic_host_info(host, host_stats, stat, sort, per_mps):

    host_data = "host: " + host.ljust(35)
    if stat == "mean" or stat == "median" or stat == "var":
        host_data += stat + " bytes/s:" + \
            str(int(host_stats['machines'][host]['total_redis_transfer_speed'][stat])).ljust(
                13)
        host_data += stat + " seconds:" + \
            str(float('%.3f' % host_stats['machines'][host][
                'total_redis_interaction_time'][stat])).ljust(10)
    else:
        host_data += stat + ":" + \
            host_stats['machines'][host]['active_interface'][
                'netstat_summary'][stat].ljust(20)
    host_data += "interface: " + \
        host_stats['machines'][host]['active_interface']['name'].ljust(10)
    host_data += "switch: " + \
        host_stats['machines'][host][
            'active_interface']['switch_name'].ljust(45)
    host_data += "port: " + \
        host_stats['machines'][host]['active_interface']['network_port']
    print host_data

    if per_mps and (stat == "mean" or stat == "median" or stat == "var"):
        host_data = ""
        for redis_server in sorted(host_stats['machines'][host]['per_redis_server_interaction_time'].iterkeys()):
            host_data = "host: " + host.ljust(50)
            host_data += "mps: " + redis_server.ljust(40)
            if sort == "time":
                if host_stats['machines'][host]['per_redis_server_interaction_time'][redis_server].get(stat):
                    host_data += stat + " seconds:" + \
                        str(float('%.3f' % host_stats['machines'][host][
                            'per_redis_server_interaction_time'][redis_server][stat]))
            elif sort == "speed":
                if host_stats['machines'][host]['per_redis_server_transfer_speed'][redis_server].get(stat):
                    host_data += stat + " bytes/s:" + \
                        str(int(host_stats['machines'][host][
                            'per_redis_server_transfer_speed'][redis_server][stat]))
            print host_data

##########################################################################


def print_netstat_data(host_stats, stat):
    tmp = {}
    for host in host_stats['machines'].iterkeys():
        if not host_stats['machines'][host]['active_interface']['netstat_summary'].get(stat):
            host_stats['machines'][host]['active_interface'][
                'netstat_summary'][stat] = 0
        tmp[host] = float(
            host_stats['machines'][host]['active_interface']['netstat_summary'][stat])
    for host in sorted(tmp, key=lambda x: tmp[x]):
        print ("host: " + host.ljust(40) +
               stat + ": " + str(host_stats['machines'][host]['active_interface']['netstat_summary'][stat]).ljust(15) +
               "interface: " + host_stats['machines'][host]['active_interface']['name'].ljust(10) +
               "switch: " + host_stats['machines'][host]['active_interface']['switch_name'].ljust(45) +
               "port: " + host_stats['machines'][host]['active_interface']['network_port'])

##########################################################################


def extract_range_clusters(host_stats):
    host_range_clusters = []
    for host in host_stats['machines'].iterkeys():
        host_range_clusters.append(
            host_stats['machines'][host]['range_clusters'])
    common_result = set(host_range_clusters[0])
    unique_result = set(host_range_clusters[0])
    for s in host_range_clusters[1:]:
        common_result.intersection_update(s)
        unique_result.symmetric_difference_update(s)
    print
    print "Common range clusters found in all above hosts".center(120)
    for cluster in sorted(list(common_result)):
        print cluster.center(120)
    print
    print "Unique range clusters found in all above hosts".center(120)
    for cluster in sorted(list(unique_result)):
        print cluster.center(120)

##########################################################################


def report_datacenter_statistics(data, stat, sort, percentile, host_stats, limit, switchcount, per_mps):

    if sort == "time":
        total_operation = "total_redis_interaction_time"
        per_operation = "per_redis_server_interaction_time"
    else:
        total_operation = "total_redis_transfer_speed"
        per_operation = "per_redis_server_transfer_speed"

    shitty_switches = {}
    print
    print ("Data collected " + str(datetime.timedelta(seconds=int(time.time() -
                                                                  host_stats['data_generation_time']))) + " minutes ago").center(120)
    print

    # For sysops-api insertion times, we take 50 samples per machine.
    # Otherwise for netstat data, the number of samples is number of hosts
    # measured.
    description = ""
    total_measurements = 0
    if stat == "mean" or stat == "median" or stat == "var":
        total_measurements = str(len(data) * 50)
        description = "sysops-api host data insertion times"
    else:
        total_measurements = str(len(data))
        description = stat

    ################################# Median #################################
    # Global median
    print ("The global median of " + description + " across " + str(len(data)) + "  hosts and " +
           total_measurements + " total measurements is " + str('%.3f' % np.median(data))).center(120)

    if stat == "mean" or stat == "median" or stat == "var":
        # Per MPS median
        if per_mps:
            for redis_server in sorted(host_stats[per_operation].iterkeys()):
                print ("The median of redis server " + redis_server + " across " + str(len(host_stats[per_operation][redis_server]['median'])) +
                       " hosts is " + str('%.3f' % np.median(host_stats[per_operation][redis_server]['median']))).center(120)
    for percent in [1, 2, 3, 4, 5, 10, 50, 80, 90, 95, 96, 97, 98, 99]:
        print (str(percent) + "th percentile median is " + str('%.3f' %
                                                               np.percentile(data, percent))).center(120)
    print
    ################################# Median #################################

    ################################# Mean ###################################
    # Global mean
    print ("The global mean of " + description + " across " + str(len(data)) + " hosts and " +
           total_measurements + " total measurements is " + str('%.3f' % np.mean(data))).center(120)

    if stat == "mean" or stat == "median" or stat == "var":
        # Per MPS mean
        if per_mps:
            for redis_server in sorted(host_stats[per_operation].iterkeys()):
                print ("The mean of redis server " + redis_server + " across " + str(len(host_stats[per_operation][redis_server]['mean'])) +
                       " hosts is " + str('%.3f' % np.mean(host_stats[per_operation][redis_server]['mean']))).center(120)
    for percent in [1, 2, 3, 4, 5, 10, 50, 80, 90, 95, 96, 97, 98, 99]:
        print (str(percent) + "th percentile mean is " + str('%.3f' %
                                                             np.percentile(data, percent))).center(120)
    print
    ################################# Mean ###################################

    # If we supply --percentile 0 or a negative number, we only want to see
    # the above summary report.  Exit after providing a client connected
    # switch count.
    if percentile <= 0:
        for host in host_stats['machines'].iterkeys():
            if not shitty_switches.get(host_stats['machines'][host]['active_interface']['switch_name']):
                shitty_switches[
                    host_stats['machines'][host]['active_interface']['switch_name']] = 1
            else:
                shitty_switches[
                    host_stats['machines'][host]['active_interface']['switch_name']] += 1
        for swatch in sorted(shitty_switches, key=lambda x: shitty_switches[x]):
            print ("Switch: " + swatch.ljust(40) + "has " + str(shitty_switches[
                   swatch]) + " clients connected as the active network interface.").center(120)
        sys.exit(0)

    # Discover which switches have hosts that have exceeded the discovered
    # limit based on --stat and --percentile
    if limit:
        for host in host_stats['machines'].iterkeys():
            if stat == "mean" or stat == "median" or stat == "var":
                # sysops-api insertion times
                if host_stats['machines'][host].get(total_operation):
                    if ((sort == "time" and float(host_stats['machines'][host][total_operation][stat]) > limit) or
                            (sort == "speed" and float(host_stats['machines'][host][total_operation][stat]) < limit)):
                        if not shitty_switches.get(host_stats['machines'][host]['active_interface']['switch_name']):
                            shitty_switches[
                                host_stats['machines'][host]['active_interface']['switch_name']] = 1
                        else:
                            shitty_switches[
                                host_stats['machines'][host]['active_interface']['switch_name']] += 1
            else:
                # netstat keys
                if host_stats['machines'][host]['active_interface']['netstat_summary'].get(stat):
                    if float(host_stats['machines'][host]['active_interface']['netstat_summary'][stat]) > limit:
                        if not shitty_switches.get(host_stats['machines'][host]['active_interface']['switch_name']):
                            shitty_switches[
                                host_stats['machines'][host]['active_interface']['switch_name']] = 1
                        else:
                            shitty_switches[
                                host_stats['machines'][host]['active_interface']['switch_name']] += 1

        for swatch in sorted(shitty_switches, key=lambda x: shitty_switches[x]):
            if shitty_switches[swatch] >= switchcount:
                print ("Switch: " + swatch.ljust(40) + str(shitty_switches[
                       swatch]) + " clients exceeded the threshold of " + str(percentile) + "th percentile of " + stat).center(120)
        print
##########################################################################


def reduce_working_set(host_stats, switch, hosts, mps):

    delete_hosts = []
    if switch:
        delete_hosts = []
        for host in host_stats['machines'].iterkeys():
            for single_switch in switch:
                if host_stats['machines'][host]['active_interface']['switch_name'] not in switch:
                    delete_hosts.append(host)

    if hosts:
        delete_hosts = []
        for host in host_stats['machines'].iterkeys():
            if host not in hosts:
                delete_hosts.append(host)

    if mps:
        delete_hosts = []
        for host in host_stats['machines'].iterkeys():
            redis_servers = host_stats['machines'][host][
                'per_redis_server_interaction_time'].keys()
            for redis_server in mps:
                if redis_server not in redis_servers:
                    delete_hosts.append(host)

    for host in delete_hosts:
        if host_stats['machines'].get(host):
            del host_stats['machines'][host]
    return host_stats
##########################################################################


def return_machines_above_limit(host_stats, stat, limit, sort, range_connection):
    # Only look at data from machines that have exceeded the limit.
    tmp = {}
    for host in host_stats['machines'].iterkeys():
        if stat == "median" or stat == "mean" or stat == "var":
            if sort == "time":
                if host_stats['machines'][host].get('total_redis_interaction_time'):
                    if float(host_stats['machines'][host]['total_redis_interaction_time'][stat]) > limit:
                        tmp[host] = host_stats['machines'][host]
            else:
                if host_stats['machines'][host].get('total_redis_transfer_speed'):
                    if float(host_stats['machines'][host]['total_redis_transfer_speed'][stat]) < limit:
                        tmp[host] = host_stats['machines'][host]
        else:
            if host_stats['machines'][host]['active_interface']['netstat_summary'].get(stat):
                if float(host_stats['machines'][host]['active_interface']['netstat_summary'][stat]) > limit:
                    tmp[host] = host_stats['machines'][host]
    host_stats['machines'] = tmp

    # Scrape range for data on all found hosts.
    for host in host_stats['machines'].iterkeys():
        try:
            host_stats['machines'][host][
                'range_clusters'] = range_connection.expand("%index:" + host)
        except Exception:
            host_stats['machines'][host]['range_clusters'] = "NOCLUSTER"

    return host_stats
##########################################################################


def report_network_data_collection(host_stats, datacenter, stat, percentile, sort, report, per_mps, switchcount, netstat, mps, switch, hosts, range_connection, range_clusters):

    limit = None
    # sysops-api insertion statistics
    if not netstat:
        if stat == "median" or stat == "mean" or stat == "var":
            if sort == "time":
                limit = float('%.3f' % np.percentile(
                    host_stats['total_redis_interaction_time'][stat], percentile))
                if report:
                    report_datacenter_statistics(host_stats['total_redis_interaction_time'][
                                                 stat], stat, sort, percentile, host_stats, limit, switchcount, per_mps)
            else:
                limit = float('%.3f' % np.percentile(
                    host_stats['total_redis_transfer_speed'][stat], percentile))
                if report:
                    report_datacenter_statistics(host_stats['total_redis_transfer_speed'][
                                                 stat], stat, sort, percentile, host_stats, limit, switchcount, per_mps)
            # Reduce the working set, if provided
            if switch or hosts or mps:
                host_stats = reduce_working_set(host_stats, switch, hosts, mps)
            host_stats = return_machines_above_limit(
                host_stats, stat, limit, sort, range_connection)
            print ("The following hosts have exceeded the " + stat + " threshold of " +
                   str(float('%.3f' % limit)) + " at the " + str(percentile) + "th percentile").center(120)
            tmp = {}
            if sort == "time":
                for host in host_stats['machines'].iterkeys():
                    tmp[host] = float(
                        host_stats['machines'][host]['total_redis_interaction_time'][stat])
                for host in sorted(tmp, key=lambda x: tmp[x]):
                    print_basic_host_info(
                        host, host_stats, stat, sort, per_mps)
            elif sort == "speed":
                for host in host_stats['machines'].iterkeys():
                    tmp[host] = float(
                        host_stats['machines'][host]['total_redis_transfer_speed'][stat])
                if stat != "var":
                    for host in reversed(sorted(tmp, key=lambda x: tmp[x])):
                        print_basic_host_info(
                            host, host_stats, stat, sort, per_mps)
                elif stat == "var":
                    for host in sorted(tmp, key=lambda x: tmp[x]):
                        print_basic_host_info(
                            host, host_stats, stat, sort, per_mps)
            elif sort == "switch":
                for host in host_stats['machines'].iterkeys():
                    tmp[host] = host_stats['machines'][host][
                        'active_interface']['switch_name']
                for host in sorted(tmp, key=lambda x: tmp[x]):
                    print_basic_host_info(
                        host, host_stats, stat, sort, per_mps)
            elif sort == "hostname":
                for host in sorted(host_stats['machines'].iterkeys()):
                    print_basic_host_info(
                        host, host_stats, stat, sort, per_mps)
            if range_clusters:
                extract_range_clusters(host_stats)

    # netstat statistics
    if netstat:
        # Every iteration modifies host_stats to reduce the working set based
        # off of the limit.  Reset host_stats on every cycle.
        saver = host_stats.copy()
        netstat_data = {}
        for stat in netstat:
            host_stats = saver.copy()
            netstat_data[stat] = []
            for host in host_stats['machines'].iterkeys():
                if host_stats['machines'][host]['active_interface']['netstat_summary'].get(stat):
                    netstat_data[stat].append(
                        host_stats['machines'][host]['active_interface']['netstat_summary'][stat])
            netstat_data[stat] = map(float, netstat_data[stat])
            if len(netstat_data[stat]) > 0:
                limit = float(
                    '%.3f' % np.percentile(netstat_data[stat], percentile))
            else:
                continue
            if report:
                report_datacenter_statistics(
                    netstat_data[stat], stat, sort, percentile, host_stats, limit, switchcount, per_mps)
            # Reduce the working set, if provided
            if switch or hosts or mps:
                host_stats = reduce_working_set(host_stats, switch, hosts, mps)
            host_stats = return_machines_above_limit(
                host_stats, stat, limit, sort, range_connection)
            print ("The following hosts have exceeded the " + stat + " threshold of " +
                   str(float('%.3f' % limit)) + " at the " + str(percentile) + "th percentile").center(120)
            # For netstat data, we process all hosts at once instead of
            # processing them individually.
            print_netstat_data(host_stats, stat)
            if range_clusters:
                extract_range_clusters(host_stats)
            print

##########################################################################
if __name__ == '__main__':

    range_connection = find_range_servers()
    sites = build_valid_sites(range_connection)

    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")
    parser.add_option("--site",
                      action="store",
                      choices=sites,
                      dest="site",
                      help="Specify a datacenter to query against.")
    parser.add_option("--report",
                      action="store_true",
                      dest="report",
                      default=None,
                      help="Show the report from the entire datacenter, which includes all discovered statistics.")
    parser.add_option("--report-switchcount",
                      action="store",
                      dest="report_switchcount",
                      type="int",
                      default=4,
                      help="The number of clients crossing the given percentile on a given switch to report as having possible upstream issues.  Default is 4")
    parser.add_option("--per-mps",
                      action="store_true",
                      dest="per_mps",
                      default=False,
                      help="Show the per-mps statistics for the datacenter, as well as insertion times when using --stat mean or --stat median")
    parser.add_option("--stat",
                      action="store",
                      dest="stat",
                      default="median",
                      choices=['median', 'mean', 'var'],
                      help="Choose between median, mode, or var (variance) as the cutoff filter for sysops-api insertion times. Default is median.")
    parser.add_option("--percentile",
                      action="store",
                      dest="percentile",
                      type="int",
                      default=1,
                      help="What percentile you want to report on?  By default, percentile is set to 1 so all host data is returned.  This is a lot of data.  You are probably interested in above 90. Set this to zero or any negative number to only view datacenter statistics")
    parser.add_option("--sort",
                      action="store",
                      dest="sort",
                      default="speed",
                      choices=['speed', 'hostname', 'time', 'switch'],
                      help="Sort results by hostname, time,  speed, switch, or netstat key.  By default, results are sorted by insertion speed.")
    parser.add_option("--range-clusters",
                      action="store_true",
                      dest="range_clusters",
                      default=False,
                      help="Include printing the common and unique range clusters for all discovered hosts")
    parser.add_option("--filter-switch",
                      action="store",
                      dest="filter_switch",
                      default=None,
                      help="Only report on host connected to a comma separated list of switches by name")
    parser.add_option("--filter-switch-file",
                      action="store",
                      dest="filter_switch_file",
                      default=None,
                      help="Only report on switches contained in the supplied file. Use - for stdin")
    parser.add_option("--filter-host",
                      action="store",
                      dest="filter_host",
                      default=None,
                      help="Only report on a comma separated list of hosts by name")
    parser.add_option("--filter-host-file",
                      action="store",
                      dest="filter_host_file",
                      default=None,
                      help="Only report on hosts contained in the supplied file.  Use - for stdin")
    parser.add_option("--filter-mps",
                      action="store",
                      dest="filter_mps",
                      default=None,
                      help="Only report on a comma separated list of hosts using mps by name")
    parser.add_option("--filter-range-query",
                      action="store",
                      dest="filter_range_query",
                      default=None,
                      help="Only report on hosts contained in the supplied range query")
    parser.add_option("--refresh",
                      action="store_true",
                      dest="refresh",
                      default=None,
                      help="Refresh the cached mean, median, and host_stat data that is used for reporting.")
    parser.add_option("--netstat",
                      action="store",
                      dest="netstat",
                      default=None,
                      help="List netstat values that you wish reported, separated by commas. Use --list-netstat or --all-netstat to see what data is available.")
    parser.add_option("--list-netstat",
                      action="store_true",
                      dest="list_netstat",
                      default=None,
                      help="List all possible netstat values which can be used with the --netstat option")
    parser.add_option("--basic-netstat",
                      action="store_true",
                      dest="basic_netstat",
                      default=None,
                      help="Print only the most interesting netstat summary data from found clients.")
    parser.add_option("--all-netstat",
                      action="store_true",
                      dest="all_netstat",
                      default=None,
                      help="Write all netstat summary data from found clients.")
    parser.add_option("--daemon",
                      action="store_true",
                      dest="daemon",
                      help="Used if the utility is supposed to be run to perform data collection")
    parser.add_option("--local-disk",
                      action="store_true",
                      dest="local_disk",
                      default=False,
                      help="Do you want to fetch datacenter results from sysops-api or compute them locally?  By default, we fetch from sysops-api.")

    (options, args) = parser.parse_args()

    global_start_time = time.time()
    host_stats = {}
    if os.geteuid() == 0:
        workdir = '/export/content/extract_network_health'
        if not os.path.exists(workdir):
            os.mkdir(workdir)
    else:
        workdir = os.getcwd()

    if not options.daemon:
        if not options.site:
            parser.print_help()
            print "\nYou must specifiy which site you want to query."
            sys.exit(1)

        if not options.report and not options.stat and not options.list_netstat and not options.basic_netstat:
            print "Choose between reporting on sysops-api insertions using --stat mean or --stat median."
            print " Otherwise, use --list-netstat or --all-netstat to find a netstat key. Then --stat <key>  to view percentiles a specific netstat value",
            parser.print_help()
            sys.exit(1)

    if options.daemon:
        options.local_disk = True
        options.refresh = True
        options.site = sites

    if type(options.site) == str:
        options.site = options.site.split()

    if options.local_disk:
        host_stats = generate_json_data(options.site, workdir, options.refresh)

    host_stats = validate_json_data(
        host_stats, options.site, options.local_disk, workdir)

    if options.list_netstat or options.all_netstat:
        options.netstat = build_valid_netstat_keys(host_stats)
        if options.list_netstat:
            for key in sorted(options.netstat):
                print key
            sys.exit(0)

    if options.basic_netstat:
        options.netstat = ['ip_dropped_because_of_missing_route',
                           'ip_fragments_created',
                           'ip_fragments_failed',
                           'ip_outgoing_packets_dropped',
                           'tcp_bad_segments_received',
                           'tcp_connection_resets_received',
                           'tcp_failed_connection_attempts',
                           'tcp_resets_sent',
                           'tcp_segments_retransmited',
                           'tcp_segments_retransmited_ratio',
                           'tcpext_connections_aborted_due_to_timeout',
                           'tcpext_connections_reset_due_to_unexpected_syn',
                           'tcpext_invalid_syn_cookies_received',
                           'tcpext_packets_dropped_from_out_of_order_queue_because_of_socket_buffer_overrun',
                           'tcpext_packets_pruned_from_receive_queue_because_of_socket_buffer_overrun',
                           'tcpext_retransmits_in_slow_start',
                           'tcpext_retransmits_in_slow_start',
                           'tcpext_sack_retransmits_failed',
                           'tcpext_tcp_data_loss_events',
                           'tcpext_tcpbacklogdrop',
                           'udp_packet_receive_errors',
                           'udp_packet_recieve_errors_ratio',
                           'udp_packets_received',
                           'udp_rcvbuferrors']

    if options.filter_host_file or options.filter_switch_file:
        if options.filter_host_file == "-":
            options.filter_host = sys.stdin.readlines()
        elif options.filter_switch_file == "-":
            options.filter_switch = sys.stdin.readlines()
        else:
            try:
                if options.filter_host_file:
                    file = open(options.filter_host_file, 'r').read()
                    options.filter_host = file.splitlines()
                if options.filter_switch_file:
                    file = open(options.filter_switch_file, 'r').read()
                    options.filter_switch = file.splitlines()
            except Exception:
                print "Could not read data from disk.  Exiting"
                sys.exit(1)

    # The map / lambda statements below attempt to deal with non-fqdn names
    # passed on the CLI.  We at least need a prod/corp/stg/fin/etc suffix to
    # append linkedin.com
    if options.filter_switch:
        options.filter_switch = options.filter_switch.split(',')
        options.filter_switch = map(lambda switch: re.sub(
            "(prod$|stg$|corp$|fin$)", r"\g<1>.linkedin.com", switch), options.filter_switch)

    if options.filter_host:
        options.filter_host = options.filter_host.split(',')
        options.filter_host = map(lambda host: re.sub(
            "(prod$|stg$|corp$|fin$)", r"\g<1>.linkedin.com", host), options.filter_host)

    if options.filter_mps:
        options.filter_mps = options.filter_mps.split(',')
        options.filter_mps = map(lambda mps: re.sub(
            "(prod$|stg$|corp$|fin$)", r"\g<1>.linkedin.com", mps), options.filter_mps)

    if options.netstat:
        if type(options.netstat) == str:
            options.netstat = options.netstat.split(',')

    if options.filter_range_query:
        options.filter_host = range_connection.expand(
            options.filter_range_query)

    if not options.daemon:
        report_network_data_collection(host_stats=host_stats,
                                       datacenter=options.site,
                                       stat=options.stat,
                                       percentile=options.percentile,
                                       sort=options.sort,
                                       report=options.report,
                                       per_mps=options.per_mps,
                                       switchcount=options.report_switchcount,
                                       netstat=options.netstat,
                                       mps=options.filter_mps,
                                       switch=options.filter_switch,
                                       hosts=options.filter_host,
                                       range_connection=range_connection,
                                       range_clusters=options.range_clusters)

        print "\nExecution completed in " + str(int(time.time() - global_start_time)) + " seconds for datacenter " + options.site[0]
    sys.exit(0)
