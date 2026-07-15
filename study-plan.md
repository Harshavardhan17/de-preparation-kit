# 4-Week Data Engineer Interview Plan

For: 4 YoE, finance data project (HSBC) — daily ledger feeds, GCS → Cloud
Composer → BigQuery. Goal: crack DE interviews with deep, honest command of
the stack she already works near.

**Daily rhythm (~2.5–3 h):** 2–3 LeetCode SQL 50 warm-ups
(`sql-practice/leetcode-sql50.md`) · 60–90 min kit SQL practice · 30 min
reading (textbook / Q&A bank) · 15 min saying answers OUT LOUD. Speaking is a
separate skill from knowing — rehearse aloud from day one.

**Starting from zero on Airflow?** (yes) — run the
`airflow-from-scratch/` track as the *evening* slot of Weeks 1–2: lessons 1–3
in Week 1, lessons 4–5 + the capstone in Week 2, lesson 7 (Composer bridge) at
the start of Week 3 right before the Q&A bank's Airflow section. Every step of
every setup is spelled out in `RUNBOOK.md` — keep it open.

---

## Week 1 — SQL foundations → window functions + project setup
- **Days 1–2:** `sql-practice/01-window-functions.md` problems 1–6. Set up the
  free BigQuery sandbox; run every solution for real, never on paper only.
- **Days 3–4:** problems 7–12 (gaps-and-islands, frames, LEAD/LAG). Read
  textbook chapter §1–2 (the business problem, the daily feed).
- **Day 5:** run `data_generator.py` for 3–4 business dates (RUNBOOK A4), load
  into sandbox staging, run `sql/01_staging_dedup.sql` by hand (RUNBOOK A6
  sandbox rewrite). Understand every line.
- **Weekend:** redo the 4 hardest problems from memory. Write her own
  "walk me through your project" draft — just bullet points.

## Week 2 — joins/aggregation + build the pipeline end-to-end
- **Days 1–2:** `sql-practice/02-joins-aggregation-dedup.md` all problems.
  The FULL OUTER JOIN recon problem is HER domain — overlearn it.
- **Days 3–4:** run `sql/02` → `05` in the sandbox in order. Break things on
  purpose: load the same day twice, corrupt an amount, delete a manifest row —
  watch which gate catches it. This is where real understanding forms.
- **Day 5:** read textbook §3–5 (Composer DAG anatomy, BigQuery layering,
  recon gate). Trace each concept to the actual DAG file.
- **Weekend:** the `airflow-from-scratch/` capstone (lesson 6) — the local
  mini ledger pipeline green end-to-end, plus its three experiments
  (idempotent rerun, DQ gate halt, late file). Optionally RUNBOOK Part C
  (real GCS bucket).

## Week 3 — BigQuery internals, Airflow theory, resume
- **Days 1–2:** `sql-practice/03-bigquery-specific.md` all problems
  (QUALIFY, MERGE, partitioning/clustering, UNNEST, cost).
- **Days 3–4:** `interview-qa.md` sections 1–3 (Airflow, BigQuery internals,
  GCS→BQ patterns). Flashcard the ones she can't answer cold.
- **Day 5:** finalize the resume (ATS pass done together). Then work through
  `resume-defense.md` with the résumé open beside it — every bullet must have a
  first-hand answer. Read Q&A §4–5 (finance domain, big data).
- **Weekend:** the `resume-defense.md` Part 3 gaps (event-driven Pub/Sub
  ingestion, GCP Data Transfer Service, Dataflow/Beam, Jenkins CI/CD, MySQL/SQL
  Server) — these cover the TCS half of her résumé the rest of the kit doesn't.
  Then textbook §7–8 (failure scenarios, whiteboarding); practice drawing the
  6-box diagram on paper in under 2 minutes, three times.

## Week 4 — mocks and gap-closing
- **Day 1:** mock SQL round (45 min, 3 problems, timed, in the sandbox).
- **Day 2:** mock pipeline-design round: "Design daily ingestion from a bank's
  upstream into a warehouse" — whiteboard + follow-ups from the Q&A bank.
- **Day 3:** mock behavioral: project narrative, failure story, the
  "you've only been there 6 months" question (coached answer in Q&A §6), and a
  résumé cross-examination — pick random bullets and defend them (Part 4 of
  `resume-defense.md` has 10 rapid-fire drills for exactly this).
- **Days 4–5:** re-drill everything marked weak in the mocks.
- **Ongoing:** 2–3 applications/day from Week 3 onward; interviews ARE
  practice — take early ones at companies she cares less about.

## Non-negotiables
1. **Type every query.** Reading solutions creates recognition, not recall.
2. **Answers out loud, daily.** Interviewers grade the explanation, not the
   silent knowledge.
3. **Honesty rule (and résumé-consistency rule):** never claim to have built
   what she didn't, and never exceed her résumé's verbs in the interview — the
   résumé says *support, build and maintain, develop*, so that is the ceiling.
   The winning frame: *"On the platform I build and maintain the ingestion DAGs
   and develop the transformations and checks; to understand the rest end-to-end
   I rebuilt a working version myself on GCP — ask me anything about it."* That
   answer beats a bluff every single time, and interviewers can tell the
   difference. See `resume-defense.md` for a line-by-line defense of every
   résumé bullet.
