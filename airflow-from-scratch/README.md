# Airflow From Scratch — hands-on track

For someone with **zero Airflow/Composer knowledge** who knows Python basics.
Seven lessons; each has a short concept read in `lessons/` and a runnable DAG in
`dags/` that you actually execute and poke at in the Airflow UI. By lesson 6
you will have built a working local mini-version of the ledger pipeline; lesson
7 maps everything you did onto Cloud Composer and the real project DAG.

**Rule for this track: never read a lesson without running its DAG.** Airflow
only makes sense once you've watched a task retry, cleared a task state, and
seen a red square turn green.

## One-time setup (~15 minutes)

Airflow runs natively on macOS/Linux. On **Windows, use WSL2** (Ubuntu) and run
the same commands inside it — plain Windows is not supported by Airflow.

```bash
# 1. A dedicated folder + virtualenv (Python 3.9–3.12)
mkdir -p ~/airflow-lab && cd ~/airflow-lab
python3 -m venv venv && source venv/bin/activate

# 2. Install Airflow WITH the constraint file (required — plain pip install breaks)
AIRFLOW_VERSION=2.10.5
PYVER="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYVER}.txt"

# 3. Point Airflow's home here and its dags_folder at this track
export AIRFLOW_HOME=~/airflow-lab/airflow
airflow standalone        # first run: initializes DB, creates admin user, prints the password
```

`airflow standalone` starts everything (scheduler + webserver + local DB) in
one process. Open **http://localhost:8080**, log in with the printed
credentials. Then stop it (Ctrl-C), edit `$AIRFLOW_HOME/airflow.cfg` and set:

```
dags_folder = /FULL/PATH/TO/de-interview-prep/airflow-from-scratch/dags
load_examples = False
```

Restart `airflow standalone`. The lesson DAGs appear in the UI within ~30s.

> Docker alternative: if you prefer containers, the official
> [docker-compose quick start](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
> works too — mount this `dags/` folder as the dags volume. Use whichever gets
> you to a green task fastest; the tool matters, the plumbing doesn't.

## The lessons

| # | Lesson | Runnable DAG | You learn |
|---|--------|--------------|-----------|
| 1 | What Airflow actually is | `l01_hello.py` | DAG / task / operator, scheduler vs webserver, reading the UI, triggering & logs |
| 2 | Operators & dependencies | `l02_operators.py` | BashOperator, PythonOperator, `>>` chains, fan-out/fan-in, trigger rules |
| 3 | Scheduling & the logical date | `l03_schedule.py` | `schedule=`, **logical date vs run date** (the #1 interview gotcha), catchup, backfill |
| 4 | Passing data: XCom & TaskFlow | `l04_taskflow_xcom.py` | `@task`, XCom under the hood, why you never pass DataFrames |
| 5 | Sensors, retries & failure | `l05_sensor_retry.py` | sensors (poke vs reschedule), retries/try_number, clearing state, callbacks |
| 6 | **Capstone: local ledger pipeline** | `l06_local_ledger_pipeline.py` | the whole project pattern — sense file → unzip → validate → load (SQLite) → DQ gate → archive — with zero cloud |
| 7 | The bridge to Cloud Composer | *(reading)* | what Composer manages for you, the GCP operators, guided read of `project/dags/ledger_pipeline_dag.py` |

Pace: comfortably 1 lesson/evening; lessons 1–3 can be one sitting. Fits the
main `study-plan.md` as evenings of Weeks 1–2 (see the plan's Airflow note).

## How to study each lesson
1. Read `lessons/0N-*.md` (10–15 min).
2. Un-pause the DAG in the UI, trigger it, watch it run, **open the logs of every task**.
3. Do the *Break it on purpose* exercises at the end of the lesson — that's
   where the learning is.
4. Say the *Interview checkpoint* answers out loud.
