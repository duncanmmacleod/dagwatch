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

"""Condor interaction utilities
"""

from __future__ import print_function

from time import sleep
from subprocess import (check_output, CalledProcessError)

import htcondor
from classad import ClassAd

JOB_STATUS = [
    'Unexpanded',
    'Idle',
    'Running',
    'Removed',
    'Completed',
    'Held',
    'Submission error',
]
JOB_STATUS_MAP = dict((v.lower(), k) for k, v in enumerate(JOB_STATUS))


def iterate_dag_status(clusterid, interval=2, schedd=None):
    """Monitor a DAG by querying condor for status information periodically

    Yields a `dict` of DAG status on each iteration, see `get_dag_status`
    for details of that `dict`.

    Parameters
    ----------
    clusterid : `float`
        the ClusterId of the DAG

    interval : `float`
        minimum time (seconds) between updates

    schedd : `htcondor.Schedd`, optional
        the open connection to the scheduler

    detailed : `bool`, optional
        check jobs as held

    See Also
    --------
    get_dag_status
        for details of the information yielded on each iteration
    """
    if schedd is None:
        schedd = htcondor.Schedd()
    while True:
        try:
            status = get_dag_status(clusterid, schedd=schedd, detailed=True)
        except (IOError, KeyError) as e:
            # reconnect, and try again
            sleep(1)
            del schedd
            schedd = htcondor.Schedd()
            try:
                status = get_dag_status(clusterid, schedd=schedd,
                                        detailed=True)
            except IOError:
                raise e
        yield status
        if 'exitcode' in status:
            break
        sleep(interval)


def get_dag_status(dagmanid, schedd=None, detailed=True):
    """Return the status of a given DAG

    Parameters
    ----------
    dagmanid : `int`
        the ClusterId of the DAG

    schedd : `htcondor.Schedd`, optional
        the open connection to the scheduler

    detailed : `bool`, optional
        check jobs as held

    Returns
    -------
    status : `dict`
        a `dict` summarising the DAG status with the following keys

        - 'total': the total number of jobs
        - 'done': the number of completed jobs
        - 'queued': the number of queued jobs (excluding held if `held=True`)
        - 'ready': the number of jobs ready to be submitted
        - 'unready': the number of jobs not ready to be submitted
        - 'failed': the number of failed jobs
        - 'held': the number of failed jobs (only non-zero if `held=True`)

        Iff the DAG is completed, the 'exitcode' of the DAG will be included
        in the returned status `dict`
    """
    # connect to scheduler
    if schedd is None:
        schedd = htcondor.Schedd()
    # find running DAG job
    states = ['total', 'done', 'queued', 'ready', 'unready', 'failed']
    classads = ['DAG_Nodes%s' % s.title() for s in states]
    try:
        job = find_job(ClusterId=dagmanid, schedd=schedd, attr_list=classads)
        status = dict()
        for s, c in zip(states, classads):
            try:
                status[s] = job[c]
            except KeyError:  # htcondor.py failure (unknown cause)
                try:
                    status[s] = int(check_output(
                        ['condor_q', str(dagmanid), '-autoformat', c]))
                except (ValueError, CalledProcessError) as e:
                    status[s] = '-'
    # DAG has exited
    except RuntimeError as e:
        if not str(e).startswith('No jobs found'):
            raise
        sleep(1)
        try:
            job = list(schedd.history('ClusterId == %s' % dagmanid,
                                      classads+['ExitCode'], 1))[0]
        except IOError:  # try again
            job = list(schedd.history('ClusterId == %s' % dagmanid,
                                      classads+['ExitCode'], 1))[0]
        except KeyError:  # condor_rm not finished yet (probably)
            sleep(10)
            job = list(schedd.history('ClusterId == %s' % dagmanid,
                                      classads+['ExitCode'], 1))[0]
        except RuntimeError as e:
            if 'timeout' in str(e).lower() or 'cowardly' in str(e).lower():
                job = get_condor_history_shell(
                    'ClusterId == %s' % dagmanid,
                    classads+['ExitCode'], 1)[0]
                job = dict((k, int(v)) for k, v in job.iteritems())
            else:
                raise
        history = dict((s, job[c]) for s, c in zip(states, classads))
        history['exitcode'] = job['ExitCode']
        history['held'] = history['running'] = history['idle'] = 0
        return history
    # DAG is running, get status
    else:
        # find node status details
        if detailed:
            status['held'] = 0
            status['running'] = 0
            status['idle'] = 0
            nodes = find_jobs(DAGManJobId=dagmanid, schedd=schedd)
            for node in nodes:
                s = get_job_status(node)
                if s == JOB_STATUS_MAP['held']:
                    status['held'] += 1
                elif s == JOB_STATUS_MAP['running']:
                    status['running'] += 1
                elif s == JOB_STATUS_MAP['idle']:
                    status['idle'] += 1
        return status


def get_condor_history_shell(constraint, classads, maxjobs=None):
    """Get condor_history from the shell

    Parameters
    ----------
    constraint : `str`
        `str` of the format 'ClassAd == "value"' defining the `-constraint`
        to pass to `condor_history`

    classads : `list` of `str`
        list of class Ad names to get back from `condor_history`

    maxjobs : `int`
        the number of matches to return

    Returns
    -------
    jobs : `list` of `dict`
        list of dicts with same keys as defined by `get_dag_status`
    """
    cmd = ['condor_history', '-constraint', constraint]
    for ad_ in classads:
        cmd.extend(['-autof', ad_])
    if maxjobs:
        cmd.extend(['-match', str(maxjobs)])
    history = check_output(' '.join(cmd), shell=True)
    lines = history.rstrip('\n').split('\n')
    jobs = []
    for line in lines:
        values = line.split()
        jobs.append(dict(zip(classads, values)))
    return jobs


def get_job_status(job, schedd=None):
    """Get the status of a HTCondor process

    Parameters
    ----------
    job : `str`, `classad.ClassAd`
        either job ID (`str`, `int`, `float`) or a job object

    schedd : `htcondor.Schedd`, optional
        open scheduler connection

    Returns
    -------
    status : `int`
        the integer (`long`) status code for this job
    """
    if not isinstance(job, ClassAd):
        job = find_job(ClusterId=job, schedd=schedd, attr_list=['JobStatus'])
    return job['JobStatus']


def find_jobs(schedd=None, attr_list=None, **constraints):
    """Query the condor queue for jobs matching the constraints

    Parameters
    ----------
    schedd : `htcondor.Schedd`, optional
        open scheduler connection

    attr_list : `list` of `str`
        list of attributes to return for each job, defaults to all

    all other keyword arguments should be ClassAd == value constraints to
    apply to the scheduler query

    Returns
    -------
    jobs : `list` of `classad.ClassAd`
        the job listing for each job found
    """
    if schedd is None:
        schedd = htcondor.Schedd()
    qstr = ' && '.join(['%s == %r' % (k, v) for
                        k, v in constraints.items()]).replace("'", '"')
    if not attr_list:
        attr_list = []
    return list(schedd.query(qstr, attr_list))


def find_job(schedd=None, attr_list=None, **constraints):
    """Query the condor queue for a single job matching the constraints

    Parameters
    ----------
    schedd : `htcondor.Schedd`, optional
        open scheduler connection

    attr_list : `list` of `str`
        list of attributes to return for each job, defaults to all

    all other keyword arguments should be ClassAd == value constraints to
    apply to the scheduler query

    Returns
    -------
    classad : `classad.ClassAd`
        the job listing for the found job

    Raises
    ------
    RuntimeError
        if not exactly one job is found matching the constraints
    """
    jobs = find_jobs(schedd=schedd, attr_list=attr_list, **constraints)
    if len(jobs) == 0:
        raise RuntimeError("No jobs found matching constraints %r"
                           % constraints)
    elif len(jobs) > 1:
        raise RuntimeError("Multiple jobs found matching constraints %r"
                           % constraints)
    return jobs[0]
