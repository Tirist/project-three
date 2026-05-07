# packages/shared/

## What belongs here
- Shared types, constants, validation schemas, and utility helpers used across multiple packages/apps.
- Cross-layer contracts for domain objects and API payload shapes.

## What does not belong here
- Business logic unique to simulation progression.
- AI prompt implementations.
- Storage-engine-specific implementations.

## Which future agents should work here
- Any implementation agent when a truly shared contract is needed.
- QA / Test Planning Agent (contract test planning).

## How this relates to the broader system
`packages/shared` prevents duplication and drift across modules. It should stay small and stable to reduce cross-team merge conflicts.
