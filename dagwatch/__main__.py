# -*- coding: utf-8 -*-
# Copyright (C) Duncan Macleod (2016)
#
# This file is part of DAGWatch.
#
# DAGWatch is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# DAGWatch is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with DAGWatch.  If not, see <http://www.gnu.org/licenses/>.

"""Watch the progress of an HTCondor workflow (DAG).
"""

import argparse
import sys

from .dagwatch import watch_dag

# -- parse command line --

parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    epilog='The exitcode of this script will match that of the watched DAG, '
           'unless KeywordInterrupted.'
)
try:
    parser._positionals.title = 'Positional arguments'
    parser._optionals.title = 'Optional arguments'
except AttributeError:
    pass

parser.add_argument('clusterid', help='ClusterId for Condor DAGMan process')
parser.add_argument('-u', '--update-interval', type=int, default=2,
                    metavar='SEC', help='interval (seconds) between updates')

pout = parser.add_argument_group('Output options')
pout.add_argument('--no-color', action='store_true', default=False,
                  help='disable coloured output')
pout.add_argument('-f', '--log-format', type=str,
                  default="[%(asctime)s] %(message)s", metavar='FORMAT',
                  help='format for logging output (default: %(default)r)')
pout.add_argument('-d', '--datetime-format', type=str,
                  default="%Y-%m-%d %H:%M:%S", metavar='DATEFMT',
                  help='format for logging date and time '
                       '(default: %(default)r)')

args = parser.parse_args()

# -- watch DAG --

try:
    exitcode = watch_dag(args.clusterid, interval=args.update_interval,
                         color=not args.no_color,
                         fmt=args.log_format, datefmt=args.datetime_format)
except KeyboardInterrupt:
    exitcode = 0

if exitcode:
    sys.exit(exitcode)
