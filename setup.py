#!/usr/bin/env python

from distutils.core import setup
setup(name="sysops-api",
      version="1.0",
      description="LinkedIn Redis / Cfengine API",
      author = "Mike Svoboda",
      author_email = "msvoboda@linkedin.com",
      py_modules=['CacheExtractor', 'RedisFinder'],
      data_files=[('/usr/local/bin', ['./scripts/extract_sysops_cache.py']),
                  ('/usr/local/bin', ['./scripts/extract_sysops_api_to_disk.py']),
                  ('/usr/local/bin', ['./scripts/extract_sysctl_live_vs_persistant_entries.py']),
                  ('/usr/local/bin', ['./scripts/extract_user_account_access.py']),
                  ('/usr/local/bin', ['./scripts/extract_user_sudo_privileges.py'])],
      package_dir={'': 'src'},
      packages = ['seco'],
      )
