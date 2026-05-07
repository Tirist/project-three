# packages/db/

## What belongs here
- Data model definitions and persistence boundaries.
- Repositories or data access contracts used by apps and packages.
- Storage concerns for companies, sessions, outcomes, and analytics.

## What does not belong here
- UI components.
- Deterministic event simulation rules.
- Prompt-writing logic.

## Which future agents should work here
- Database Agent (primary)
- QA / Test Planning Agent (data integrity tests)

## How this relates to the broader system
`packages/db` stores and retrieves simulation-related entities. It should expose stable interfaces so `packages/core` and `apps/web` are not tightly coupled to storage details.
