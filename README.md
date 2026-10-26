# Evidence pipeline

A multi-step agentic workflow runner. Standard library only, no install, no network, no API key.

```
python3 workflow.py
```

Built for the We The Flywheel Sr Agentic Engineer assessment. Full run output in `sample-output.txt`.

## The problem

A personal site whose stated thesis is "show the work, and let the evidence carry it" publishes
activity panels drawn from several sources: commits, model token use, training load. The panels
already label their own honesty in several dimensions. Estimates are marked as estimates. Untracked
days are marked as not-zero. An incomplete day says so.

Freshness is the dimension that is labelled on only one panel, and that panel promises a weekly
refresh.

So this pipeline has two gates, and they guard opposite ends of the same failure:

| Gate | Catches |
|---|---|
| **freshness** (input) | a figure that is TRUE but too old to describe in the present tense |
| **verification** (output) | a figure that is CURRENT but traces to no record |

Most pipelines build the second and skip the first. The first is the one that catches a missed
refresh before it becomes a claim on a public page.

## Steps

`collect` to `normalise` to `freshness` to `select` to `draft` to `verify` to `queue`

Exactly one of those steps is model-authored. `draft` writes the sentence. Everything else is
deterministic code: what gets collected, what counts as news, what the budget is, what reaches a
human.

## The design positions

Each is marked `[D<n>]` at its implementation in the source.

| | Position |
|---|---|
| **D1** | The model decides wording. Code decides selection, ordering, budget and publication. |
| **D2** | Budget is enforced by the runner against a deadline, outside the agent. A step cannot opt out of its own cap. |
| **D3** | Cancellation is caught as `BaseException`, not `Exception`. |
| **D4** | The verification gate fails closed, and a gate failure is terminal, never retried. |
| **D5** | Run state is persisted per step AND the queue write carries an idempotency key. |
| **D6** | Publication under a person's name is a human gate. The pipeline queues, it does not post. |
| **D7** | Every step emits a span with duration and token cost. |
| **D8** | A stale source is excluded and alerted, not halted on and not silently included. |

### D4 is the one worth arguing

The gate does not ask the model to be truthful. It extracts every numeric literal from the draft and
refuses any figure that does not trace to a collected measurement, its baseline, or a delta this code
derived from the two. Formatting cannot hide a figure: `1,200,000` and `1.2M` both resolve to the
same value. Comparison runs at twelve significant digits, because at the default six an invented
`1200004` compares equal to a real `1200000` and walks straight through.

The second half of D4 is subtler. **A transient step is worth retrying. A gate is not.** Retrying a
verification gate until it passes is how you get flaky truth, so `GateFailed` propagates past the
retry loop and halts the run.

### D8, and why a stale source does not halt

One cold source must not block three warm ones, and must not ride along inside the word "this week".
So a source past its SLO is dropped from the narratable set and alerted by name. Only all sources
cold halts the run, because there is then nothing true left to say.

### D5, and why the state file alone is not enough

Resume-from-state skips completed steps. That is not sufficient. If the process dies after the queue
backend accepts the write but before the state file records it, the resumed run queues a second copy.
The idempotency key closes that window on the backend side, which is the only side that can close it.
`sample-output.txt` reproduces exactly that death and ends with `queue depth after both runs: 1`.

## What it deliberately does not do

- **No date or ordinal allowance in the gate.** "Week 43" and "top 3" block. Telling a year from an
  invented figure needs context the gate does not have, so it fails closed and a draft that wants a
  date has to get it from a record.
- **No parallelism.** The steps are a chain. Fan-out needs partial-failure semantics, and guessing at
  those is worse than leaving it out.
- **No model-chosen control flow.** The model fills one step. It does not pick the next one. That is a
  deliberate ceiling on blast radius, not a missing feature.
- **No real persistence.** Run state is a JSON file. Production wants a store with compare-and-set so
  two runners cannot claim the same run.
