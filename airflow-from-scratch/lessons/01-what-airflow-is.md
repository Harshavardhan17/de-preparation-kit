# Lesson 1 — What Airflow actually is

**Run:** `l01_hello` (un-pause it in the UI, press ▶ Trigger, watch it turn green)

## The problem Airflow solves

Your team's job: every morning a file arrives, and a *sequence of steps* must
happen — wait for it, unzip it, validate it, load it, check it, forward it.
You could write one big Python script with a cron entry. But then: what
happens when step 3 fails at 2 a.m.? Which day's runs failed last week? How do
you rerun *only* step 4 for *only* last Tuesday? Who gets alerted?

**Airflow is a framework for running scheduled, dependent steps with retries,
history, and a UI** — cron plus memory, plus a control panel. Cloud Composer
(lesson 7) is simply Google running Airflow for you.

## The four words you must own

- **DAG** (Directed Acyclic Graph): one pipeline — a set of tasks and the
  arrows between them. "Directed" = arrows have a direction; "acyclic" = no
  loops (a task can't depend on itself). In code: one Python file defining a
  `DAG(...)` object. Your team's daily ledger pipeline = one DAG.
- **Task**: one node in the graph — one unit of work ("unzip the file").
- **Operator**: the *template* that a task is made from. `BashOperator` runs a
  shell command, `PythonOperator` calls a Python function,
  `GCSToBigQueryOperator` loads a bucket file into a table. Task = operator +
  parameters. *(This is what your team means by "we use some operators.")*
- **DAG run**: one execution of the whole DAG for one data interval — "the
  run for July 14". Each square in the UI grid is one task in one run.

## What's actually running on your machine

`airflow standalone` started three things:

| Component | Job |
|---|---|
| **Scheduler** | The brain. Reads your DAG files every ~30s, decides which tasks are due, hands them to workers. |
| **Webserver** | The UI at localhost:8080. Purely a window — killing it doesn't stop pipelines. |
| **Metadata DB** | Every DAG run, task attempt, state and XCom lives here. The UI is just a view over this database. |

In Composer these same three run on Google-managed infrastructure — nothing
conceptually new.

## Do this now
1. Un-pause `l01_hello` (toggle left of the name) and trigger it.
2. Click the run → **Graph** view: three boxes, two arrows. That's the DAG.
3. Click `say_hello` → **Log**. Find the `Hello from inside a PythonOperator!`
   line. *Logs are where you'll live as a data engineer.*
4. Click `print_date` → note the state lifecycle it went through:
   `queued → running → success`.

## Break it on purpose
- Edit `l01_hello.py`: change the bash command to `datee` (typo). Wait ~30s,
  trigger again, watch `print_date` go red, read the log, fix it, then use
  **Clear** on the failed task — see how Airflow re-runs *just that task* and
  continues. Clearing state is the #1 daily operation in real teams.

## Interview checkpoint (say these out loud)
- *What is a DAG?* — A pipeline defined as tasks with directed, non-circular
  dependencies; Airflow schedules and tracks each run of it.
- *Difference between a task and an operator?* — An operator is the reusable
  class; a task is a configured instance of one inside a DAG.
- *What does the scheduler do?* — Parses DAG files, creates DAG runs on
  schedule, and queues tasks whose upstreams have succeeded.
