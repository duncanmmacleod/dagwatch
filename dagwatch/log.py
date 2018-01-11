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

"""Utilities for logging output from Omicron in python
"""

import logging
import sys

try:
    from lal import GPSTimeNow as gps_time_now
except ImportError:
    from gwpy.time import tconvert as gps_time_now

COLORS = dict((c, 30 + i) for i, c in enumerate(
    ['black', 'red', 'green', 'yellow',
     'blue', 'magenta', 'cyan', 'white']))
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"


def bold(text):
    """Format a string of text as bold for the shell

    Simply returns "\033[1m{text}\033[0m"
    """
    return ''.join([BOLD_SEQ, text, RESET_SEQ])


def color_text(text, color):
    if not isinstance(color, int):
        color = COLORS[color]
    return COLOR_SEQ % color + str(text) + RESET_SEQ
