# apps/web/

## What belongs here
- Mobile-web-first client application.
- Interaction flows for voice-first operation with tap/click fallback.
- Session lifecycle screens (start day, in-day events, end-of-day debrief).

## What does not belong here
- Deterministic simulation engine internals.
- Prompt templates or AI orchestration logic.
- Database migration definitions.

## Which future agents should work here
- Frontend Agent (primary)
- Voice Interaction Agent (UI event handling integration)
- Product UX Agent

## How this relates to the broader system
`apps/web` is the presentation shell. It renders state from `packages/core`, sends user inputs, and displays narration/evaluation from `packages/ai`.
