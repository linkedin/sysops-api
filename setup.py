#!/usr/bin/env python

from distutils.core import setup
setup(name="sysops-api",
      version="1.0",
      description="LinkedIn Redis / Cfengine API",
      author="Mike Svoboda",
      author_email="msvoboda@linkedin.com",
      scripts=['scripts/extract_sysops_cache.py',
               'scripts/extract_sysops_api_to_disk.py',
               'scripts/extract_sysctl_live_vs_persistant_entries.py',
               'scripts/extract_user_account_access.py',
               'scripts/extract_user_sudo_privileges.py'],
      packages=['seco', 'sysopsapi'],
      )
