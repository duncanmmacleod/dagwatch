# -*- coding: utf-8 -*-
# The MIT License (MIT)
# Copyright (c) 2018 Duncan Macleod
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

"""Watch the progress of an HTCondor workflow (DAG).
"""

import argparse
import sys

from . import __version__
from .dagwatch import watch_dag

__author__ = 'Duncan Macleod <duncan.macleod@ligo.org>'

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

parser.add_argument('-V', '--version', action='version', version=__version__)

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
