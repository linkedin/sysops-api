'''
Build information from ldap entries that have a member key
'''
import collections
import os
import re
import site
import sys
import yaml

TOOLS_LIBS = '/usr/local/linkedin/lib/python2.6/site-packages'
if os.path.exists(TOOLS_LIBS):
  site.addsitedir(TOOLS_LIBS)
else:
  raise NotImplementedError("Libraries missing from {0} for LDAP".format(TOOLS_LIBS))

from linkedin.utils.ldap import LDAP

def sync(args):
  if 'top_group' in args:
    return get_members(args.get('groups', []), args.get('top_group'))
  return get_members(args.get('groups', []))

def get_members(ldap_groups, top_group='ldap.members'):
  '''
  Sync info from ldap
  '''
  # Match a common name out of a distinguished name
  cn_re = re.compile(r'CN=([^,]*),')

  if top_group.endswith('.'):
    top_group = top_group.rstrip('.')

  ld = LDAP()

  range_data = collections.defaultdict(dict)
  range_data[top_group] = {'CLUSTER': set()}

  for group in ldap_groups:
    try:
      range_data[top_group]['CLUSTER'].add(group)
      if top_group + group not in range_data:
        range_data['.'.join((top_group, group))] = { 'CLUSTER': set() }
      for dn in ld.find_by_common_name(group).get('member', []):
        match = cn_re.search(dn)
        uid = ld.find_by_common_name(match.group(1))['sAMAccountName'][0]
        range_data['.'.join((top_group, group))]['CLUSTER'].add(uid)
    except AttributeError as e:
      print "Error looking up LDAP group {0}: ".format(group)
      raise

  return range_data

if __name__ == '__main__':
  args = {}
  args['groups'] = [
    'engrna-cde',
    'engrna-cnc',
    'engrna-core',
    'engrna-data',
    'engrna-dds',
    'engrna-mobile',
    'engrna-money',
    'engrna-search',
    'engrna-security',
    'engrna-si',
    'engrna-sna',
    'engrna-tools'
  ]

  #args['top_group'] = 'ldap.engrna'

  range_data = sync(args)
  yaml.dump(range_data, sys.stdout, default_flow_style=False, indent=4)
