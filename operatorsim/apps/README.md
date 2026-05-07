# apps/

## What belongs here
- End-user applications that can be run or deployed.
- UI surfaces for trainees, managers, and admins.
- App-level routing, screens, and app composition.

## What does not belong here
- Core simulation rules and deterministic domain logic.
- Shared cross-app utilities that should live in `packages/shared`.
- Database schema or persistence adapters.

## Which future agents should work here
- Frontend Agent
- Voice Interaction Agent (UI integration portions)
- Product UX Agent
- QA / Test Planning Agent (app-level test plans)

## How this relates to the broader system
`apps/` consumes libraries from `packages/`. It should present simulation state and AI outputs, but not own business truth.
