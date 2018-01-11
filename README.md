# DAGWatch

The `dagwatch` python package allows users to track the progress of a HTCondor DAGMan workflow from the command line.

## Simple usage

To follow a DAG, simply find the `ClusterId` of the parent `condor_dagman` job, then execute `dagwatch`

```bash
$ python -m dagwatch 123456.0
[2018-01-11 03:37:56] --------------------------------------------------------------------
[2018-01-11 03:37:56] Monitoring workflow 123456
[2018-01-11 03:37:56] DAG_NodesTotal: 21
[2018-01-11 03:37:56] Machine: deepthought.example.com
[2018-01-11 03:37:56] Owner: whitemice
[2018-01-11 03:37:56] JobBatchName: ultimatequestion+123456
[2018-01-11 03:37:56] --------------------------------------------------------------------
[2018-01-11 03:37:56] unready |   ready |    idle | running |    held |  failed |    done
[2018-01-11 03:37:56] --------------------------------------------------------------------
[2018-01-11 03:37:56]       0 |       4 |       0 |       1 |       0 |       0 |      16
[2018-01-11 03:38:00]       0 |       2 |       0 |       2 |       0 |       0 |      17
...
... some time later
...
[2018-01-11 05:00:00]       0 |       0 |       0 |       0 |       0 |       0 |      21
[2018-01-11 05:00:00] DAG has exited with exitcode 0
```
