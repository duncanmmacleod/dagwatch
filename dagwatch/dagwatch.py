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

"""Condor interaction utilities
"""

from __future__ import print_function

import logging

from .condor import (find_job, iterate_dag_status)
from .log import color_text

NODE_STATES = ['unready', 'ready', 'idle', 'running', 'held', 'failed', 'done']
NODE_COLORS = ['white', 'magenta', 'white', 'blue', 'yellow', 'red', 'green']

LOGGING_DEFAULTS = {
    'fmt': "[%(asctime)s] %(message)s",
    'datefmt': "%Y-%m-%d %H:%M:%S",
}


def watch_dag(clusterid, interval=2, color=True, logger=None, **logger_kw):
    """Print progress of DAGMan workflow at regular intervals

    At each interval, a progress line is printed to the ``logger`` displaying
    the number of unready, ready, idle, running, held, failed, and done
    nodes in the DAG workflow.

    Parameters
    ----------
    clusterid : `int`
        the ClusterId ClassAd for the DAGMan process

    interval : `float`, optional
        time (seconds) between updates, default: ``2``

    color : `bool`, optional
        print coloured output, default: `True`

    logger : `logging.Logger`, optional
        logger to print updates to, one will be created if not given

    **logger_kw
        keyword arguments for the new `logging.Formatter`,
        only used if `logger=None`

    Returns
    -------
    exitcode : `int`
        the exit code of the DAG when complete
    """
    clusterid = float(clusterid)

    # set up logger
    if logger is None:
        logger = logging.Logger(str(clusterid))
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        lkw = LOGGING_DEFAULTS.copy()
        lkw.update(logger_kw)
        handler.setFormatter(logging.Formatter(**lkw))

    # disable coloured output
    if color:
        color = color_text
    else:
        def color(text, null):
            return text

    # find DAGMan process
    job = dict(find_job(
        ClusterId=float(clusterid),
        attr_list=['Owner', 'Machine', 'ClusterId', 'JobBatchName',
                   'DAG_NodesTotal']))
    for key in ('ServerTime', 'MyType', 'TargetType',):  # remove useless keys
        job.pop(key, None)

    # print logging header
    logger.info('-' * 68)
    logger.info("Monitoring workflow {}".format(job.pop('ClusterId')))
    for key, val in job.items():
        logger.debug("{}: {}".format(key, val))
    logger.info('-' * 68)
    logger.info(
        '{0} |   {1} |    {2} | {3} |    {4} |  {5} |    {6}'.format(
        *[color(s, c) for s, c in zip(NODE_STATES, NODE_COLORS)]))
    logger.info('-' * 68)

    # iterate status, only update on changes
    old = dict((k, -1) for k in NODE_STATES)
    for job in iterate_dag_status(clusterid, interval=interval):
        for state in NODE_STATES:
            if job[state] != old[state]:
                logger.info(' | '.join(['%7s' % job[s] for s in NODE_STATES]))
                break
        old = job

    # print footer
    logger.info('-' * 68)
    if job['exitcode']:
        logger.critical("DAG has exited with status %d" % job['exitcode'])
    else:
        logger.info("DAG has exited with status %d" % job['exitcode'])
    return job['exitcode']
