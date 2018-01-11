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
