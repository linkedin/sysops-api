"""
Various core functions for dealing with merging and normalizing data that will
be added to range
"""
# Core
import re
import collections

# 3rd party

# Local
import seco.range
from linkedin.utils.logger import get_logger
logger = get_logger(__name__)

_RANGE_SERVER = 'localhost:80'
_UNKNOWN_CLUSTER_NAME = 'engrna-unknown'

def nested_set():
  return collections.defaultdict(set)

def sync(config):
  range_server = config.get('range_server', _RANGE_SERVER)
  group_re_text = config.get('group_re', 'ldap.members.engrna')
  logger.debug("Group RE: {0}".format(group_re_text))
  group_re = re.compile(group_re_text)
  range = seco.range.Range(range_server)

  all_clusters = None
  try:
    all_clusters = range.expand("allclusters()")
  except seco.range.RangeException as e:
    # TODO really need to bomb out at this point.
    print "Error: Could not fetch 'allclusters()'."

  ldap_groups = [ grp for grp in all_clusters if group_re.search(grp) ]

  access_groups = collections.defaultdict(set)
  for ld_group in ldap_groups:
    logger.debug("LDAP Group: {0}".format(ld_group))
    a_group = ld_group.split('.')[-1]
    for person in range.expand("%" + ld_group):
      logger.debug("Member of '{0}': {1}".format(ld_group, person))
      access_groups[person].add(a_group)

  access_clusters = collections.defaultdict(nested_set)
  for cluster in all_clusters:
    owners = set()
    hosts = None
    try:
      owners.update(range.expand("%" + cluster + ":OWNERS"))
    except seco.range.RangeException as e:
      pass
    try:
      owners.update(range.expand("%" + cluster + ":WAR_OWNERS"))
    except seco.range.RangeException as e:
      pass
    try:
      hosts = range.expand("%" + cluster)
    except seco.range.RangeException as e:
      pass
    if owners and hosts:
      for owner in owners:
        # Check to see if the owner is in one of the ldap groups
        cur_groups = access_groups.get(owner)
        if cur_groups:
          for grp in cur_groups:
            agrp = 'access.' + grp
            access_clusters['access']['CLUSTER'].add(agrp)
            access_clusters[agrp]['CLUSTER'].update(hosts)
        else:
          logger.debug("Skipping owner: {0}".format(owner))
          pass
    elif hosts:
      logger.debug("Cluster with no owners: {0}".format(cluster))
      # Place holder for feature for svoboda
      pass
  return access_clusters
