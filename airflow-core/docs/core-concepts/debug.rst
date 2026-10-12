 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _concepts:debugging:

Debugging Airflow Dags
======================

Testing Dags with dag.test()
-----------------------------

To debug Dags in an IDE, you can set up the ``dag.test`` command in your Dag file and run through your Dag in a single
serialized python process.

This approach can be used with any supported database (including a local SQLite database) and will
*fail fast* as all tasks run in a single process.

``dag.test()`` executes a real Dag run, so it has two prerequisites:

* an initialized metadata database (``airflow db migrate``), and
* a Dag that Airflow can serialize, which means the Dag must be defined in a file inside a
  configured :doc:`Dag bundle </administration-and-deployment/dag-bundles>` — by default, your
  Dags folder. ``dag.test()`` re-parses the bundle and serializes the Dag for you.

A Dag built in memory — for instance inside a pytest function, in a file outside your Dags folder —
is not in any bundle, so nothing gets serialized and the call fails with
``Cannot create DagRun for DAG <dag_id> because the dag is not serialized``. To test operator
behaviour without a Dag run at all, call the operator directly as described in
:ref:`Unit tests <best_practices:unit_tests>`.

To set up ``dag.test``, add these two lines to the bottom of your Dag file:

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

and that's it! You can add optional arguments to fine tune the testing but otherwise you can run or debug Dags as
needed. Here are some examples of arguments:

* ``logical_date`` if you want to test argument-specific Dag runs
* ``use_executor`` if you want to test the Dag using an executor. By default ``dag.test`` runs the Dag without an
  executor, it just runs all the tasks locally.
  By providing this argument, the Dag is executed using the executor configured in the Airflow environment.

Conditionally skipping tasks
----------------------------

If you don't wish to execute some subset of tasks in your local environment (e.g. dependency check sensors or cleanup steps),
you can automatically mark them successful supplying a pattern matching their ``task_id`` in the ``mark_success_pattern`` argument.

In the following example, testing the Dag won't wait for either of the upstream Dags to complete. Instead, testing data
is manually ingested. The cleanup step is also skipped, making the intermediate csv is available for inspection.

.. code-block:: python

  with DAG("example_dag", default_args=default_args) as dag:
      sensor = ExternalTaskSensor(task_id="wait_for_ingestion_dag", external_dag_id="ingest_raw_data")
      sensor2 = ExternalTaskSensor(task_id="wait_for_dim_dag", external_dag_id="ingest_dim")
      collect_stats = PythonOperator(task_id="extract_stats_csv", python_callable=extract_stats_csv)
      # ... run other tasks
      cleanup = PythonOperator(task_id="cleanup", python_callable=Path.unlink, op_args=[collect_stats.output])

      [sensor, sensor2] >> collect_stats >> cleanup

  if __name__ == "__main__":
      ingest_testing_data()
      run = dag.test(mark_success_pattern="wait_for_.*|cleanup")
      print(f"Intermediate csv: {run.get_task_instance('collect_stats').xcom_pull(task_ids='collect_stats')}")


Debugging Airflow Dags on the command line
==========================================

With the same two line addition as mentioned in the above section, you can now easily debug a Dag using pdb as well.
Run ``python -m pdb <path to Dag file>.py`` for an interactive debugging experience on the command line.

.. code-block:: bash

  [Breeze:3.11.16] root@ef2c84ad4856:/opt/airflow# python -m pdb providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(18)<module>()
  -> """Example Dag demonstrating the usage of the BashOperator."""
  (Pdb) b 45
  Breakpoint 1 at /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py:45
  (Pdb) c
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(45)<module>()
  -> task_id="run_after_loop",
  (Pdb) run_this_last
  <Task(EmptyOperator): run_this_last>

**IDE setup steps:**

1. Add ``main`` block at the end of your Dag file to make it runnable.

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

2. Run / debug the Dag file.


Debugging Dags in a deployed environment
========================================

``dag.test()`` and ``pdb`` are useful while you write a Dag, but most problems show up later, in a shared
deployment, with real Connections, real data and a schedule. The techniques below help you find out why a task
failed there and confirm a fix without re-running the whole Dag.

Start from the failed task instance
-----------------------------------

In the :ref:`Grid view <ui-grid-view>`, click the failed task cell to open the
:ref:`Task Instance view <ui-ti-view>`. Its :ref:`tabs <ui-ti-tabs>` answer most first questions:

* **Logs** shows the output and traceback of the attempt. If the task was retried, select the attempt you are
  interested in, because the first failure is often more informative than the last one.
* **Rendered Templates** shows the templated fields after Jinja rendering. Check it when a task received the wrong
  date, path, SQL or parameter: the problem is often in the template, not in the operator.
* **XCom** shows what the task pushed or returned. Open it on the *upstream* task as well, to confirm that the
  failing task received the input you expected.
* **Details** shows the Dag version and bundle the task ran with, and **Code** shows the Dag source at that version.
  Use them to rule out "the fix was deployed, but this run used the old code".

Inspect a task instance from the command line
---------------------------------------------

The same information is available from the CLI, which is convenient on a worker or in a script. Each command takes
the Dag ID, the task ID and the logical date or run ID of the Dag run:

.. code-block:: bash

  # Current state of the task instance
  airflow tasks state my_dag my_task manual__2026-01-01T00:00:00+00:00

  # Why the scheduler does not queue it (unmet dependencies, pool, concurrency limits, ...)
  airflow tasks failed-deps my_dag my_task manual__2026-01-01T00:00:00+00:00

  # Templated fields as they render for that run
  airflow tasks render my_dag my_task manual__2026-01-01T00:00:00+00:00

``failed-deps`` is the quickest way to debug a task that stays in the ``scheduled`` or ``queued`` state instead
of failing. For mapped tasks, pass ``--map-index``.

If a Dag is missing from the UI or has stopped updating, check whether its file fails to import:

.. code-block:: bash

  airflow dags list-import-errors

Check Connections and Variables
-------------------------------

Many "works locally, fails in production" errors come from Connections or Variables that are missing or differ
between environments. Check what the deployment actually has:

.. code-block:: bash

  airflow connections get my_conn_id
  airflow variables get my_variable

If your deployment enables connection testing (``[core] test_connection = Enabled``; it is disabled by default),
``airflow connections test my_conn_id`` also checks that the connection can reach its service. See
:doc:`/howto/connection` and :doc:`/howto/variable` for how Connections and Variables are resolved.

Re-run a single task
--------------------

``airflow tasks test`` runs one task instance in the current process, without checking its dependencies and
without recording its state in the database. Use it on a machine that has the same configuration, Connections and
packages as your workers to reproduce a failure:

.. code-block:: bash

  airflow tasks test my_dag my_task manual__2026-01-01T00:00:00+00:00

Useful options:

* ``-t`` / ``--task-params`` passes a JSON dict of params to the task.
* ``-m`` / ``--post-mortem`` opens a debugger when the task raises an uncaught exception.
* ``--map-index`` selects one instance of a mapped task.

.. warning::

  ``airflow tasks test`` really executes the task. If the task writes to a database, calls an API or deletes
  files, those side effects happen. Point it at test data or a non-production Connection where you can.

Once you have a fix, re-run the task inside the deployment by **clearing** it, so the scheduler runs it again and
records the result. In the UI, select the task instance and use **Clear**, optionally including downstream tasks.
From the CLI:

.. code-block:: bash

  # Clear only failed instances of the matching tasks, and everything downstream of them
  airflow tasks clear my_dag -t "^my_task$" -s 2026-01-01 -e 2026-01-01 --only-failed --downstream

If a task cannot succeed for a reason outside the Dag (for example, data that will be delivered later by another
process), you can instead **mark** it as successful or failed in the UI so that downstream tasks can continue. Use
this deliberately: marking a task successful does not run it.
