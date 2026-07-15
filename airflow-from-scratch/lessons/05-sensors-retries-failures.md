# Lesson 5 — Sensors, retries & failure handling

**Run:** `l05_sensor_retry` — this one is interactive; follow the steps below.

## Sensors: tasks that wait

A **sensor** is an operator whose only job is to answer, repeatedly: *"can we
proceed yet?"* — file exists? partition landed? other DAG finished? It pokes
every `poke_interval` seconds until true, or fails at `timeout`.

The setting interviewers probe — **`mode`**:

| mode | Behavior | Cost |
|---|---|---|
| `poke` (default) | task keeps its worker slot the whole wait | a 4-hour wait occupies a worker for 4 hours |
| `reschedule` | releases the slot between pokes; re-queues itself | slot used only during each check — use this for long waits |
| deferrable operators | the modern async version: the wait moves to a lightweight *triggerer* process, no worker at all | best; e.g. `GCSObjectExistenceSensor(deferrable=True)` |

The ledger DAG waits up to 4 hours for the feed — `reschedule` (or deferrable)
is the only sane choice, and saying *why* is an instant credibility point.

## Retries: transient vs real failures

Cloud things fail randomly — a 500 from the API, a network blip. `retries=2,
retry_delay=timedelta(minutes=5)` makes those self-heal; only *persistent*
failures escalate to humans. `on_failure_callback` fires **after retries are
exhausted** — that's where Slack/PagerDuty/email alerts hook in. An **SLA**
(`sla=timedelta(hours=2)`) alerts when a task finishes *too late* even if it
succeeded.

## Do this now (the choreography)
1. Trigger the DAG. Watch `wait_for_file` sit in `up_for_reschedule` — check
   the worker isn't occupied (it disappears from running task instances
   between pokes).
2. In a terminal: `mkdir -p ~/airflow-lab/landing && touch ~/airflow-lab/landing/ready.txt`
3. Within ~15s the sensor goes green. Then `flaky_load` runs, **fails**
   (attempt 1), enters `up_for_retry` (orange), waits 30s, retries, succeeds.
4. Read `flaky_load`'s log for attempt 1 AND attempt 2 (log tabs per try).
   You have now watched Airflow absorb a transient failure with zero human
   involvement — the point of the whole system.

## Break it on purpose
- Delete `ready.txt`, set `timeout=60`, trigger, and let the sensor time out.
  Watch the failure callback print its ALERT line in the log.
- Change `flaky_load` to fail on every attempt (`if True:`). Observe: 3 total
  tries (1 + 2 retries), then red, then the callback. This is what a real
  page-at-3am looks like in the UI.

## Interview checkpoint
- *Poke vs reschedule?* — poke holds the worker slot for the whole wait;
  reschedule frees it between checks; deferrable operators move the wait off
  workers entirely.
- *How do you handle a feed that sometimes arrives hours late?* — reschedule
  sensor with a generous timeout + SLA alert on the DAG so humans hear about
  lateness even before the timeout; downstream stays blocked, nothing partial
  is published.
- *Retries vs alerting?* — retries absorb transient faults silently; the
  failure callback fires only when retries are exhausted — page on real
  problems, not blips.
