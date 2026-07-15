# Data Engineer Interview Prep Kit

Prep kit for a 4-YoE data engineer (finance / HSBC: daily CSV+ZIP ledger
feeds → GCS → Cloud Composer → BigQuery) targeting Data Engineer roles.
Strategy: instead of memorizing answers about a pipeline she only partially
touches at work, she **rebuilds the whole pipeline herself in miniature** —
then every interview question about "her" pipeline is about something she
actually built.

## Contents

| Path | What | When |
|---|---|---|
| `study-plan.md` | The 4-week day-by-day plan. **Start here.** | Day 0 |
| `RUNBOOK.md` | Click-by-click master runbook: sandbox setup, running every script, connecting to GCS — assumes zero prior setup. Keep it open always. | Day 0 |
| `airflow-from-scratch/` | Zero-to-Airflow hands-on track: 7 lessons, each with a runnable DAG, ending in a local mini ledger pipeline and a Composer bridge. | Weeks 1–2 evenings |
| `textbook/ledger-pipeline-end-to-end.md` | Textbook chapter: the entire daily ledger pipeline as a domain — business problem → feed → DAG → BigQuery → recon → audit, plus whiteboarding script. | Weeks 1–3 reading |
| `project/` | The portfolio project: data generator, Composer/Airflow DAG, 5 transformation SQL scripts, runnable on the free BigQuery sandbox. | Weeks 1–2 hands-on |
| `sql-practice/` | 32 advanced SQL problems (BigQuery, finance-framed): window functions · joins/aggregation/dedup · BigQuery-specific. Plus `04-mysql-sqlserver-dialects.md` (cross-dialect drills — her résumé lists all three DBs) and `leetcode-sql50.md` (LeetCode Top SQL 50 companion). | Daily |
| `interview-qa.md` | Q&A bank: Airflow/Composer, BigQuery internals, GCS→BQ patterns, ledger domain, big data, and behavioral coaching (including the "only 6 months on the project" answer). | Weeks 3–4 |
| `recently-asked-questions.md` | Field-collected questions actually asked in recent DE interviews (GCP roles, banks), cross-referenced into the kit. | Weeks 3–4 |
| `resume-defense.md` | Line-by-line defense of every résumé bullet + the topics the résumé claims that the rest of the kit under-covers (event-driven Pub/Sub ingestion, GCP Data Transfer Service, Dataflow/Beam, Jenkins CI/CD, MySQL/SQL Server dialects). **Read alongside the résumé.** | Week 3, and before every interview |

## The core idea

Interviewers at the 4-year level probe **depth on your own resume**: What
happens when the file is late? Duplicated? How is the load idempotent? Why is
the table partitioned that way? The kit exists so that every one of those has
a first-hand answer. The honesty rule (see `study-plan.md`) is the strategy:
own the real slice, demonstrate the whole via the rebuilt project.

## Setup in 10 minutes
1. Sign up for the free [BigQuery sandbox](https://cloud.google.com/bigquery/docs/sandbox) (no credit card).
2. `python3 project/data_generator.py --date 2026-07-14`
3. Open `study-plan.md`, start Week 1.
# de-preparation-kit
