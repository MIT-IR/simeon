"""
simeon is a python package and CLI tool that helps with fetching and processing edX research data.
"""
import logging


__version__ = '0.0.21'

# Set loggers for the scripts and package users
# 1. for simeon
logging.getLogger('SIMEON')
# 2. simeon-geoip
logging.getLogger('SIMEON-GEOIP')
# 3. simeon-youtube
logging.getLogger('SIMEON-YOUTUBE')
