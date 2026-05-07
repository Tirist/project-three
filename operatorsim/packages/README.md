# packages/

## What belongs here
- Reusable system layers used by one or more apps.
- Deterministic simulation engine, AI orchestration layer, data layer abstractions, and shared types.

## What does not belong here
- App-specific UI code.
- One-off scripts and ad hoc tooling.

## Which future agents should work here
- Simulation Core Agent
- AI Prompting Agent
- Database Agent
- Scoring Agent
- QA / Test Planning Agent (package test strategy)

## How this relates to the broader system
`packages/` contains the product backbone. `apps/` should depend on these packages rather than duplicating logic.
