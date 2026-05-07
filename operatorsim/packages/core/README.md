# packages/core/

## What belongs here
- Deterministic simulation logic and domain rules.
- Game-day progression engine (time slices, triggers, event sequencing).
- Event resolution rules and state transitions.
- Scoring inputs and deterministic metric calculations.

## What does not belong here
- LLM prompt text, response parsing, or AI provider integration.
- UI presentation components.
- Persistence-specific query code.

## Which future agents should work here
- Simulation Core Agent (primary)
- Scoring Agent (logic contributions)
- QA / Test Planning Agent (core test matrix)

## How this relates to the broader system
`packages/core` is the source of truth for simulation state. Other layers may describe or evaluate events, but must not overwrite deterministic outcomes.
