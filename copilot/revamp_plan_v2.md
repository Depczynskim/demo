# Pops Analytics Copilot – Revamp Plan v2

## 1  First principles

• **Value-centric, not volume-centric** – Every metric, model and paragraph is judged by its ability to sharpen the signal on purchase-intent (≃ "valuable traffic").

• **Hermeneutic loop** – Insight is an iterative conversation. The system observes, interprets, reflects, re-interprets; we formalise that loop in code.

• **Polyphony over monologue** – For every LLM output we embed an internal *critic* that challenges omissions and lazy inferences.

• **Memory with humility** – Past recommendations and change-log inform, but never dictate; the system must recognise when yesterday's truth has expired.

---

## 2  Three pillars of the new Copilot

### Pillar A  Valuable-traffic canon

• Single canonical classifier (rule-based → ML later) tags each session as *valuable* or *rubbish*.

• All analytics thereafter quote **split metrics** – no raw counts without their valuable/rubbish counterpart.

### Pillar B  Subject micro-narratives

• *Subject* = coherent lens (product, geography, campaign, emerging trends…).

• Each subject outputs a data-package (metrics + 12-month history) **and** a short conversational narrative.

• The *critic* reviews each narrative for blind-spots and data gaps before acceptance.

### Pillar C  Dialogue composer

• Micro-narratives feed a combined summary; that summary feeds back to revise micro-narratives – one or two hermeneutic turns until convergence.

• Recommendations are produced **after** convergence, with critic ensuring they are actionable, non-trivial, and provenance-linked to insights.

• Final artefact = report designed for human dialogue (sections you can expand, play as audio, or query in chat).

---

## 3  Operating cadence

| Stage | Cadence |
|-------|---------|
| Data refresh & tagging | Every 30 days (rolling 12-month window) |
| Report generation      | On-demand or scheduled post-refresh |
| Change-log updates     | Continuous via Streamlit widget |

Deterministic JSON + Markdown artefacts allow cheap, repeatable regeneration.

---

## 4  Tech posture

• Retain FastAPI + Streamlit scaffolding; replace old batch pipeline with new subject loaders.

• Version-control models & prompts; outputs are *typed JSON* so component upgrades are safe.

• Default LLM: **o3-mini** for both generation and critic (different system prompts). Upgrade path open.

---

## 5  Success heuristics

1. Precision of *valuable* label ≥ 80 % while covering an acceptable share of converters.
2. Narratives that pass critic without manual edits ≥ 90 %.
3. Stakeholders judge recommendations "immediately actionable".
4. ↓ Report reading time • ↓ Decision-to-action latency.

---

## 6  Discussion checkpoints

When we resume work, align on:

a) Which engagement signals constitute *value* in this domain.

b) Which subjects deserve first-class status out of the gate.

c) Number of hermeneutic iterations that balances insight depth vs compute cost.

> *This blueprint anchors the **why**.  Implementation details can now evolve without losing the strategic arc.* 

---

## 7  Why this approach works (strengths)

1. **Value-first metric** – Anchoring all analytics on *valuable vs rubbish* traffic curbs vanity metrics.
2. **Hermeneutic loop** – Author → Critic iterations reduce hallucinations while staying cost-efficient.
3. **Modularity & provenance** – Layered pipeline and artefact snapshots let us unit-test, diff and replay.
4. **Business-context feedback** – Change-log and memory injection bridge the gap between data and on-site actions.

## 8  Watch-outs & risk mitigation

| Area | Potential issue | Mitigation |
|------|-----------------|-----------|
| Ground-truth labels | Proxy events may misclassify value | Ingest real sales / high-quality leads wherever possible; monitor precision/recall monthly |
| Loop depth | Too many hermeneutic passes → cost ↑ insight ↓ | Default to **2** passes; make depth configurable via env var |
| Critic prompt | Vague feedback leads to loops | Use a checklist-style prompt (missing KPIs? ambiguous trend?) |
| Model & TTS cost | Unbounded cost at scale | Cache LLM + audio outputs; cap token & audio lengths |
| Change-log UX | Users stop logging if UX heavy | Keep widget minimal: date + free-text note |
| Regression risk | New pipeline may regress insights | Keep legacy 30/90/365 scripts behind a feature flag until full parity verified |
| Anomaly scenarios | Zero valuable sessions or absurd cost per valuable | Add smoke-test; abort report and alert |

> These guard-rails should travel with the implementation to preserve intent as the system evolves. 