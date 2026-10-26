"""
Evidence pipeline: a multi-step agentic workflow runner.

The problem it solves: a personal site whose stated thesis is "show the work,
and let the evidence carry it" publishes activity panels from several sources.
Panels already label estimates, untracked days and incomplete days. Freshness
is labelled on only one panel, and that panel promises a weekly refresh.

So the pipeline has two gates, not one, and they guard opposite ends:

  freshness gate (input)  - a figure that is TRUE but too old to narrate
  verification gate (output) - a figure that is CURRENT but traces to nothing

Most pipelines build the second and skip the first. The first is the one that
catches a missed refresh before it becomes a claim on a public page.

Design positions, each marked [D<n>] at its implementation:

  [D1] The model decides wording. Code decides selection, ordering, budget and
       publication. Ranking is arithmetic, not taste.
  [D2] Budget is enforced by the runner against a deadline, outside the agent.
       A step cannot opt out of its own cap.
  [D3] Cancellation is caught as BaseException. A deadline that kills its own
       fallback is not a fallback.
  [D4] The verification gate fails CLOSED. No figure reaches publication unless
       it traces to a collected or code-derived record.
  [D5] Run state is persisted per step, so a resume after a mid-run death
       cannot double-publish or re-spend the model budget.
  [D6] Publication under a person's name is a human gate. The pipeline queues.
  [D7] Every step emits a span with duration and cost.
  [D8] A stale source is excluded and alerted, not halted on and not silently
       included. One cold source must not block three warm ones, and must not
       ride along inside the word "this week".

Runs on the standard library alone:  python3 answers/q2-workflow.py
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable


# ---------------------------------------------------------------- budget ----

class BudgetExceeded(Exception):
    """Raised by the runner, never by a step."""


class GateFailed(Exception):
    """
    A verification gate rejected the work.

    Terminal by design. A transient step is worth retrying; a gate is not.
    Retrying a gate until it passes is how you end up with flaky truth, so this
    propagates untouched past the retry loop and halts the run.
    """


class NothingToSay(Exception):
    """Every source is stale. Publishing a roundup now would be a lie."""


@dataclass
class Budget:
    """[D2] Caps live on the runner, not inside the agent loop."""

    wall_seconds: float
    max_tokens: int
    started: float = field(default_factory=time.monotonic)
    tokens_used: int = 0

    def remaining(self) -> float:
        return self.wall_seconds - (time.monotonic() - self.started)

    def check(self, step: str) -> None:
        if self.remaining() <= 0:
            raise BudgetExceeded(f"wall-clock budget exhausted before {step}")
        if self.tokens_used >= self.max_tokens:
            raise BudgetExceeded(f"token budget exhausted before {step}")

    def spend(self, tokens: int) -> None:
        self.tokens_used += tokens


# ----------------------------------------------------------------- trace ----

@dataclass
class Span:
    step: str
    ok: bool
    seconds: float
    tokens: int
    note: str = ""


class Trace:
    """[D7] Per-step spans with cost attribution."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.spans: list[Span] = []

    def record(self, span: Span) -> None:
        self.spans.append(span)
        flag = "ok " if span.ok else "FAIL"
        print(f"  [{flag}] {span.step:<14} {span.seconds:6.3f}s "
              f"{span.tokens:>5} tok  {span.note}")

    def total_tokens(self) -> int:
        return sum(s.tokens for s in self.spans)


# ------------------------------------------------------------------ tools ---

@dataclass
class Tool:
    """A tool is a named, scoped callable. Scopes are allowlisted per run."""

    name: str
    scope: str
    fn: Callable[..., Any]


class ToolRegistry:
    def __init__(self, allowed_scopes: set[str]) -> None:
        self._tools: dict[str, Tool] = {}
        self._allowed = allowed_scopes

    def register(self, tool: Tool) -> None:
        if tool.scope not in self._allowed:
            raise ValueError(
                f"tool {tool.name!r} wants scope {tool.scope!r}, "
                f"which this run does not grant"
            )
        self._tools[tool.name] = tool

    def call(self, name: str, **kwargs: Any) -> Any:
        if name not in self._tools:
            raise KeyError(f"no tool {name!r} in this run's registry")
        return self._tools[name].fn(**kwargs)


# ------------------------------------------------------------------ state ---

class RunState:
    """[D5] Step output persisted on completion, so a resume skips done work."""

    def __init__(self, path: str, run_id: str) -> None:
        self.path = path
        self.run_id = run_id
        self.data: dict[str, Any] = {"run_id": run_id, "steps": {}}
        if os.path.exists(path):
            with open(path) as fh:
                self.data = json.load(fh)

    def done(self, step: str) -> bool:
        return step in self.data["steps"]

    def get(self, step: str) -> Any:
        return self.data["steps"][step]

    def put(self, step: str, value: Any) -> None:
        self.data["steps"][step] = value
        with open(self.path, "w") as fh:
            json.dump(self.data, fh, indent=2)


# ------------------------------------------------------------- the steps ----
# Each step is a plain function. Nothing here knows it is in an agent pipeline.

def collect(registry: ToolRegistry, _prev: Any) -> list[dict]:
    """Pull raw records from every evidence source."""
    records: list[dict] = []
    for source in ("commits", "tokens", "training"):
        records.extend(registry.call(f"fetch_{source}"))
    return records


def normalise(_registry: ToolRegistry, records: list[dict]) -> list[dict]:
    """One event schema. Heterogeneous sources are the usual source of drift."""
    return [
        {
            "source": r["source"],
            "metric": r["metric"],
            "value": r["value"],
            "baseline": r.get("baseline"),
            "unit": r.get("unit", ""),
            "observed_at": r["observed_at"],
        }
        for r in records
    ]


def freshness_gate(registry: ToolRegistry, events: list[dict],
                   slo_days: int) -> list[dict]:
    """
    [D8] The gate on the input side, and the one most pipelines never build.

    A source past its freshness SLO is not an error and not a crash. It is a
    source that may no longer be described in the present tense. It is dropped
    from the narratable set and alerted on by name, so the miss is visible as a
    miss rather than quietly riding along inside the word "this week".

    One cold source does not block the warm ones. All of them cold is the only
    case that halts, because there is then nothing true left to say.
    """
    fresh, stale = [], []
    for event in events:
        (stale if _age_days(event["observed_at"]) > slo_days else fresh).append(event)

    for event in stale:
        registry.call(
            "alert",
            subject=f"stale source: {event['source']}",
            detail=(f"{event['metric']} last observed {event['observed_at']}, "
                    f"{int(_age_days(event['observed_at']))} days ago, "
                    f"SLO is {slo_days} days"),
        )

    if not fresh:
        raise NothingToSay(
            f"all {len(stale)} sources are past the {slo_days}-day freshness SLO"
        )
    return fresh


def select(_registry: ToolRegistry, events: list[dict]) -> list[dict]:
    """
    [D1] Deterministic. The model does not choose what is newsworthy.

    Ranked by movement against each metric's own baseline, not by raw value.
    Raw value across mixed units is meaningless: a token count in the millions
    outranks every run and every commit forever, so the same panel wins every
    week and the pipeline has no news sense at all.

    The delta is computed here, in code, which is what makes it quotable later.
    """
    ranked = []
    for event in events:
        baseline = event.get("baseline")
        if not baseline:
            continue
        change = (event["value"] - baseline) / baseline * 100
        ranked.append({**event,
                       "delta_pct": abs(round(change)),
                       "direction": "up" if change >= 0 else "down"})
    ranked.sort(key=lambda e: e["delta_pct"], reverse=True)
    return ranked[:3]


def draft(registry: ToolRegistry, events: list[dict]) -> str:
    """The only model-authored step in the pipeline."""
    return registry.call("llm_draft", events=events)


def verify(_registry: ToolRegistry, payload: tuple[str, list[dict]]) -> str:
    """
    [D4] Fails closed.

    Every numeric literal in the draft must trace to a record the pipeline
    actually holds: a collected measurement, its baseline, or a delta this
    code derived from the two. A figure the model reasoned its way to is not
    traceable and does not pass, however plausible it reads.
    """
    text, events = payload
    supported: set[str] = set()
    for event in events:
        supported.add(_canon(event["value"]))
        if event.get("baseline") is not None:
            supported.add(_canon(event["baseline"]))
        if event.get("delta_pct") is not None:
            supported.add(_canon(event["delta_pct"]))

    unsupported = _figures(text) - supported
    if unsupported:
        raise GateFailed(
            "draft asserts figures with no source record: "
            + ", ".join(sorted(unsupported))
        )
    return text


def queue_for_review(registry: ToolRegistry, text: str) -> dict:
    """
    [D6] A human gate. The pipeline never publishes under his name.

    [D5] The key is the run, not the attempt. State-based resume alone is not
    enough: if the process dies after the backend accepts the write but before
    the state file records it, the resumed run queues a second copy. The key
    closes that window on the backend side, which is the only side that can
    actually close it.
    """
    return registry.call("enqueue", text=text, status="awaiting_human")


def _age_days(iso_day: str) -> float:
    stamp = time.mktime(time.strptime(iso_day, "%Y-%m-%d"))
    return (time.time() - stamp) / 86400


# A figure as a human writes it: 1,200,000 or 1.2M or 47. Digits glued to a
# letter on their left (Q3, v2) are identifiers, not figures. Years and
# ordinals ("week 43", "top 3") still count, and block: the gate fails closed.
_FIGURE = re.compile(r"(?<![\w.])(\d[\d,]*(?:\.\d+)?)([kKmM](?![a-zA-Z]))?")
_SCALE = {"k": 1e3, "m": 1e6}


def _figures(text: str) -> set[str]:
    """Every figure in the text, as a value, so formatting cannot hide one."""
    found = set()
    for digits, suffix in _FIGURE.findall(text):
        value = float(digits.replace(",", ""))
        if suffix:
            value *= _SCALE[suffix.lower()]
        found.add(_canon(value))
    return found


def _canon(value: Any) -> str:
    """
    So that 4 and 4.0 are the same number to the gate. Twelve significant
    digits, not the default six: at six, an invented 1200004 compares equal to
    a real 1200000 and walks straight through.
    """
    return f"{float(value):.12g}"


# ----------------------------------------------------------------- runner ---

@dataclass
class Step:
    name: str
    fn: Callable[[ToolRegistry, Any], Any]
    retries: int = 0
    cost_tokens: int = 0


def run(steps: list[Step], registry: ToolRegistry, budget: Budget,
        state: RunState, trace: Trace) -> Any:
    value: Any = None
    for step in steps:
        if state.done(step.name):                       # [D5] resume
            value = state.get(step.name)
            trace.record(Span(step.name, True, 0.0, 0, "skipped, already done"))
            continue

        budget.check(step.name)                         # [D2]
        try:
            value = _run_one(step, registry, budget, trace, value)
        except (GateFailed, NothingToSay):
            # Not an incident. The pipeline worked and decided not to publish,
            # which is the outcome it exists to produce. Nobody gets paged for
            # a gate doing its job.
            raise
        except Exception as exc:
            # A step that exhausted its retries IS an incident. Escalate by
            # name rather than letting the run die into a log nobody reads.
            registry.call("alert", subject=f"step failed: {step.name}",
                          detail=str(exc))
            raise
        state.put(step.name, value)
    return value


def _run_one(step: Step, registry: ToolRegistry, budget: Budget,
             trace: Trace, value: Any) -> Any:
    last_error: Exception | None = None

    for attempt in range(step.retries + 1):
        started = time.monotonic()
        try:
            result = step.fn(registry, value)
            budget.spend(step.cost_tokens)
            trace.record(Span(step.name, True, time.monotonic() - started,
                              step.cost_tokens))
            return result

        except (GateFailed, NothingToSay) as exc:       # [D4] terminal
            trace.record(Span(step.name, False, time.monotonic() - started, 0,
                              f"gate rejected: {exc}"))
            raise

        except Exception as exc:                        # recoverable
            last_error = exc
            trace.record(Span(step.name, False, time.monotonic() - started, 0,
                              f"attempt {attempt + 1}: {exc}"))
            if attempt < step.retries:
                time.sleep(0.05 * (2 ** attempt))       # backoff

        except BaseException as exc:                    # [D3]
            # Cancellation and timeouts do NOT inherit from Exception. Caught
            # above, they walk straight past every recovery path and out of the
            # request. Record the span, then re-raise: a cancel is not a retry.
            trace.record(Span(step.name, False, time.monotonic() - started, 0,
                              f"cancelled: {type(exc).__name__}"))
            raise

    tries = step.retries + 1
    raise RuntimeError(f"step {step.name!r} failed after {tries} "
                       f"attempt{'s' if tries != 1 else ''}") from last_error


# --------------------------------------------------------- fake backends ----
# Stand-ins so the pipeline runs offline. Swap for real clients at this seam.

_TODAY = time.strftime("%Y-%m-%d")


def _days_ago(n: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(time.time() - n * 86400))


def _fetch_commits() -> list[dict]:
    return [{"source": "git", "metric": "commits_this_week", "value": 47,
             "baseline": 31, "unit": "commits", "observed_at": _TODAY}]


def _fetch_tokens() -> list[dict]:
    return [{"source": "llm", "metric": "tokens_this_week", "value": 1200000,
             "baseline": 1500000, "unit": "tokens", "observed_at": _TODAY}]


def _fetch_training(stale: bool = False) -> list[dict]:
    return [{"source": "intervals_icu", "metric": "km_this_week", "value": 62,
             "baseline": 55, "unit": "km",
             "observed_at": _days_ago(21) if stale else _TODAY}]


def _llm_draft(events: list[dict], honest: bool = True) -> str:
    """A real client goes here. Deterministic so the gate is demonstrable."""
    lead = events[0]
    body = (f"{lead['metric'].replace('_', ' ')} {lead['direction']} "
            f"{lead['delta_pct']} percent, at {lead['value']} {lead['unit']}.")
    rest = [f"{e['value']} {e['unit']}" for e in events[1:]]
    if rest:
        body += " Also this week: " + ", ".join(rest) + "."
    if not honest:
        body += " Engagement up 35 percent."      # the figure nothing backs
    return body


def _make_queue(run_id: str, dies_once: bool):
    """A queue backend that dedupes on an idempotency key. Shared across the
    death and the resume, the way a real backend would be."""
    ledger: dict[str, dict] = {}
    died = {"yet": False}

    def enqueue(text: str, status: str) -> dict:
        key = f"{run_id}:queue"
        if key in ledger:
            print(f"       DEDUPE accepted key {key} already, not queueing twice")
            return ledger[key]
        ledger[key] = {"queued_at": _TODAY, "status": status,
                       "text": text, "key": key}
        if dies_once and not died["yet"]:
            died["yet"] = True
            raise ConnectionError("accepted the write, then the connection dropped")
        return ledger[key]

    return enqueue, (lambda: {"depth": len(ledger)})


def _alert(subject: str, detail: str) -> dict:
    print(f"       ALERT  {subject}: {detail}")
    return {"paged": True, "subject": subject}


def build_registry(honest_model: bool = True, stale_training: bool = False,
                   queue_dies_once: bool = False,
                   run_id: str = "run") -> ToolRegistry:
    reg = ToolRegistry(allowed_scopes={"read:evidence", "model",
                                       "write:queue", "ops:alert"})
    reg.register(Tool("fetch_commits", "read:evidence", _fetch_commits))
    reg.register(Tool("fetch_tokens", "read:evidence", _fetch_tokens))
    reg.register(Tool("fetch_training", "read:evidence",
                      lambda: _fetch_training(stale_training)))
    reg.register(Tool("llm_draft", "model",
                      lambda events: _llm_draft(events, honest_model)))
    reg.register(Tool("alert", "ops:alert", _alert))

    enqueue, depth = _make_queue(run_id, queue_dies_once)
    reg.register(Tool("enqueue", "write:queue", enqueue))
    reg.register(Tool("queue_depth", "write:queue", depth))
    return reg


def build_steps(slo_days: int = 7) -> list[Step]:
    return [
        Step("collect", collect, retries=2),
        Step("normalise", normalise),
        Step("freshness", lambda r, e: freshness_gate(r, e, slo_days)),
        Step("select", select),
        # draft and verify are paired so the gate can see the source records
        Step("draft", lambda r, events: (draft(r, events), events),
             retries=1, cost_tokens=800),
        Step("verify", verify),
        Step("queue", queue_for_review),
    ]


def _execute(label: str, registry: ToolRegistry, path: str, run_id: str) -> None:
    print(f"\n=== run: {label} ===")
    trace = Trace(run_id)
    try:
        result = run(build_steps(), registry,
                     Budget(wall_seconds=30, max_tokens=5000),
                     RunState(path, run_id), trace)
        print(f"  -> queued for human review: {result['text']!r}")
    except GateFailed as exc:
        print(f"  -> BLOCKED before publish: {exc}")
    except NothingToSay as exc:
        print(f"  -> NOTHING PUBLISHED: {exc}")
    except BudgetExceeded as exc:
        print(f"  -> halted: {exc}")
    except Exception as exc:
        print(f"  -> run died: {type(exc).__name__}: {exc}")
    finally:
        print(f"  spans={len(trace.spans)} tokens={trace.total_tokens()}")


def main() -> None:
    scenarios = [
        ("honest model", {}),
        ("model invents a figure", {"honest_model": False}),
        ("training source 21 days stale", {"stale_training": True}),
    ]
    for label, kwargs in scenarios:
        run_id = str(uuid.uuid4())[:8]
        path = f"/tmp/run-{run_id}.json"
        try:
            _execute(label, build_registry(run_id=run_id, **kwargs), path, run_id)
        finally:
            if os.path.exists(path):
                os.remove(path)

    # [D5] The worst-case death: the backend ACCEPTS the write, then the
    # process dies before the state file records it. Resume re-runs the queue
    # step, and only the idempotency key stops a second copy going out.
    run_id = str(uuid.uuid4())[:8]
    path = f"/tmp/run-{run_id}.json"
    registry = build_registry(queue_dies_once=True, run_id=run_id)
    try:
        _execute("backend accepts, process dies before state is written",
                 registry, path, run_id)
        _execute("same run resumed after the death", registry, path, run_id)
        print(f"  queue depth after both runs: "
              f"{registry.call('queue_depth')['depth']}")
    finally:
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    main()
