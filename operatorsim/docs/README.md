# docs/

## What belongs here
- Product requirements, architecture rationale, domain definitions, and implementation planning notes.
- Agent coordination docs and scope boundaries.

## What does not belong here
- Executable code.
- Prompt runtime assets (those belong in `prompts/`).

## Which future agents should work here
- Architecture Agent
- Product UX Agent
- QA / Test Planning Agent
- Any agent proposing major system changes

## How this relates to the broader system
`docs/` is the coordination layer. Agents should check these documents before implementing features to avoid conflicts and duplicated decisions.
