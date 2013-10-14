"""
Function dealing with building range data from Glu
"""
import collections
import logging
import os
import re
import shutil
import socket
import sys
import tempfile
import threading
import time
import cPickle as pickle
from Queue import Queue

# 3rd party
import pysvn

# Local
import li.sre.threadpool as threadpool

if os.path.isdir('/usr/local/linkedin/lib/python2.6/site-packages'):
  import site
  site.addsitedir('/usr/local/linkedin/lib/python2.6/site-packages')
else:
  raise NotImplementedError("No /usr/local/linkedin libraries, you can't talk to glu")

import linkedin.constants
import linkedin.tools.fabric
import seco.range.sync.constants
from linkedin.deployment.topology import topology_from_http, Application
from linkedin.mint.war import WarInfo
from linkedin.person import Person, PersonError

log = logging.getLogger(__name__)
people = dict()

class OwnerException(Exception):
  pass

def sync(args):
  """
  Fetch and build range data from glu models

  Args:
    model-repo: The subversion URL to pull down glu model files
  """
  all_clusters = {}

  cache_for = args.get('owner-cache-time', 14400)
  cache_file = args.get('owner-cache-file', '/tmp/owner-cache.pkl')
  max_threads = args.get('max-threads', 30)
  ldap_server = args.get('ldap-server')

  owners = None
  if os.path.exists(cache_file):
    file_stats = os.stat(cache_file)
    now = time.time()
    if not (now - file_stats.st_mtime) > cache_for:
      with open(cache_file) as fh:
        try:
          owners = pickle.load(fh)
        except Exception as e:
          print e
          owners = None

  if not owners:
    owners = get_owners(max_threads, ldap_server)
    with open(cache_file, 'w') as fh:
      pickle.dump(owners, fh)

  fabrics = linkedin.constants.FABRICS
  # Useful for testing one fabric.
  # TODO really need to make --fabric a command line argument.
  #fabrics = ['STG-BETA']
  for fabric in fabrics:
    log.info("Parsing glu data for fabric: {0}".format(fabric))
    clusters = None
    filename = None
    try:
      filename = _get_fabric_model(fabric, svn_url=args['model-repo'])
      if filename is None:
        sys.stderr.write('Could not retrieve the model for %s\n' % fabric)
        continue
      fabric = linkedin.tools.fabric.Fabric(fabric, use_live=False, filename=filename)
      clusters = _build_container_clusters(fabric, owners)
      all_clusters.update(clusters)
    except TypeError, e:
      print "Could not parse fabric '%s': %s" % (fabric, e)
      raise
    finally:
      if filename and os.path.exists(filename):
        shutil.rmtree(os.path.dirname(filename))
  return all_clusters

def _get_fabric_model(fabric, model_dir=None,
    svn_url='http://svn.corp.linkedin.com/relrepo/deployment/models/'):
  """
  Download a fabric model from subversion and parse it
  """

  fabric = linkedin.tools.fabric.normalize_fabric_name(fabric)

  model_file = '{0}.static.json'.format(fabric)
  svn_url = svn_url + model_file

  if not model_dir:
    model_dir = tempfile.mkdtemp(prefix="{0}-model-".format(fabric))

  model_file = os.path.join(model_dir, "{0}.json".format(fabric))

  svn_client = pysvn.Client()
  head = pysvn.Revision(pysvn.opt_revision_kind.head)
  svn_client.export(svn_url, model_file, revision=head, native_eol=None, force=True)

  return model_file

def _build_container_clusters(fabric, owners):
  """
  Build a data structure from product API that maps range keys to PAPI information
  """
  # Import interally to avoid circular imports
  from seco.range.sync import norm_key
  clusters = collections.defaultdict(dict)
  fsn = fabric.short_name
  if fsn == 'stg':
    fsn = 'alpha'
  elif fsn == 'stg-beta':
    fsn = 'beta'

  if fsn not in clusters:
    clusters[fsn] = collections.defaultdict(set)

  # Host level tagging clusters
  tag_cluster = '{0}.{1}'.format(fsn, 'tag_hosts')
  host_cluster = '{0}.{1}'.format(fsn, 'host_tags')
  if tag_cluster not in clusters:
    clusters[tag_cluster] = collections.defaultdict(set)
    clusters[tag_cluster]['CLUSTER'] = None
  if host_cluster not in clusters:
    clusters[host_cluster] = collections.defaultdict(set)
    clusters[host_cluster]['CLUSTER'] = None

  for product in fabric.products:
    product_cluster = '{0}.product.{1}'.format(fsn, product.name)
    if product_cluster not in clusters:
      clusters[product_cluster] = collections.defaultdict(set)
    clusters[fsn]['CLUSTER'].add(product_cluster)

    for container in product.containers:
      container_cluster = '{0}.{1}'.format(fsn, container.name)
      if container_cluster not in clusters:
        clusters[container_cluster] = collections.defaultdict(set)
      clusters[product_cluster]['CLUSTER'].add(container_cluster)

      for service in container.services:
        instance = None
        try:
          instance = int(re.match(r'^i(\d+)$', service.id).group(1))
        except Exception as e:
          # If the instance cannot be matched, the only way we can figure out
          # the number to attached to the cluster is via some kludges.
          # We should assume that the upstream data is bad at this point and just bail.
          # TODO need to emit a metric: range-sync.fatal.bad-instance
          log.fatal("No match for instance: {0}.{1} = {2}".format(service.fabric.name, service.name, service.id))
          sys.exit(seco.range.sync.constants._EXIT_BAD_INSTANCE_NAME)
        cluster_name = '{0}.{1}.{2}'.format(fsn, container.name, instance)
        if service.host.name == '*.linkedin.com':
          continue
        if cluster_name not in clusters:
          clusters[cluster_name] = collections.defaultdict(set)
        clusters[cluster_name]['CLUSTER'].add(service.host.name)
        try:
          clusters[cluster_name]['CONFIG'].add("-".join(
              service.config2_url.split('/')[-2:]))
        except AttributeError:
          # Might not have this
          pass
        clusters[cluster_name]['CONTAINER'] = container.name
        clusters[cluster_name]['CONTEXTPATH'].add(service.context)
        clusters[cluster_name]['DRMODE'] = container.dr_mode
        clusters[cluster_name]['FABRIC'] = fabric.name
        clusters[cluster_name]['INSTANCE'] = instance
        if container.port:
          clusters[cluster_name]['CONTAINERPORT'] = container.port
        clusters[cluster_name]['PRODUCT'] = product.name
        clusters[cluster_name]['PRODUCTVERSION'] = product.version
        clusters[cluster_name]['SERVICE'].add(service.name)
        clusters[cluster_name]['TAGS'].update([t.name for t in container.tags])
        clusters[cluster_name]['TAGS'].update([t.name for t in service.tags])
        clusters[cluster_name]['WARS'].add(service.warname)
        clusters[cluster_name]['WARVERSIONS'].add("{0}-{1}".format(
          service.warname, service.version))

        clusters[container_cluster]['CLUSTER'].add(cluster_name)

        try:
          # Host level tags
          for tag in service.host.tags:
            curr_tag = norm_key(tag.name)
            clusters[host_cluster][service.host.name].add(curr_tag)
            clusters[tag_cluster][curr_tag].add(service.host.name)
        except AttributeError, e:
          print "Couldn't get host-level tags: %s" % e

        if container.name in owners:
          clusters[cluster_name]['WAR_OWNERS'] = owners[container.name][0]
          clusters[cluster_name]['WAR_GROUPS'] = [ norm_key(group) for group in owners[container.name][1] ]
        # Looks like the "owners" code is now spitting out unicode.
        # This is creating some minor issues.
        # This is an ugly hack, but don't have time for anything more elegant.
        if 'WAR_OWNERS' in clusters[cluster_name]:
          clusters[cluster_name]['WAR_OWNERS'] = set([str(owner) for owner in clusters[cluster_name]['WAR_OWNERS']])

  return clusters

def get_owners(max_threads=30, ldap_server=None):
  '''
  Get owner information and add to range
  '''
  # Get the topology once
  log.info("Fetching topology...")
  topology = topology_from_http()
  log.info("Topology fetch done.")

  owner_queue = Queue()
  lock = threading.Lock()

  services = set()
  range_data = {}

  for container in topology.find_models(Application):
    for war in container.wars:
      services.add((container, war.artifact))

  if services:
    tp = threadpool.ThreadPool(max_threads)
    for container, service in services:
      tp.add_task(_get_owners, container, service, owner_queue, lock,
          ldap_server)
    tp.wait_completion()

    while not owner_queue.empty():
      q_item = owner_queue.get()
      if q_item == 'fault':
        raise OwnerException('Fault while retrieving owners')
      for container_name, owner_info in q_item.items():
        range_data[container_name] = owner_info

  return range_data

def _get_owners(container, service, queue, lock, ldap_server=None):
  warinfo = WarInfo()
  owners_set = set()
  owner_groups_set = set()
  owners = None
  try:
    owners = warinfo.owners_for_war(service, container.product)
  except socket.error as e:
    print "Internal Thread Fault", e
    queue.put('fault')
    raise


  for owner in owners:
    uid = owner.replace('@linkedin.com', '')
    try:
      person = None
      group = None
      if uid in people:
        person = people[uid]
      else:
        if ldap_server:
          raise NotImplementedError('LDAP servers can not yet be specified')
          # TODO: Match whatever the API changes to
          person = Person(uid, ldap_server)
        else:
          person = Person(uid)
        with lock:
          global people
          people[uid] = person
      if person:
        group = person.department
        owners_set.add(uid)
        owner_groups_set.add(group)
    except PersonError, e:
      with lock:
        global people
        people[uid] = None
      log.critical("Couldn't lookup up uid '{0}': {1}".format(uid, e))

  payload = {container.name: (owners_set, owner_groups_set)}
  queue.put(payload)
