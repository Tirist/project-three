# Agent Work Plan

This plan defines independent tracks so agents can work concurrently with minimal overlap.

## 1) Frontend Agent
**Responsibilities**
- Build mobile-web trainee experience.
- Implement session lifecycle UI and event interaction screens.
- Integrate voice-first interaction controls at UI level.

**Owns**
- `apps/web/`

**Should avoid**
- `packages/core/` simulation rules
- `packages/ai/` prompt content
- `packages/db/` schema contracts

**Expected deliverables**
- Responsive mobile-first interface.
- Event interaction flow connected to package interfaces.
- Debrief presentation for session outcomes.

## 2) Simulation Core Agent
**Responsibilities**
- Implement deterministic simulation state model and transitions.
- Build game-day progression and trigger engine.
- Define event lifecycle mechanics.

**Owns**
- `packages/core/`

**Should avoid**
- UI rendering files in `apps/web/`
- Prompt logic in `packages/ai/`

**Expected deliverables**
- Testable simulation engine modules.
- Event trigger and resolution logic.
- State transition documentation updates.

## 3) AI Prompting Agent
**Responsibilities**
- Design prompt structures for narration, dialogue, and coaching.
- Define structured output expectations for evaluation artifacts.

**Owns**
- `packages/ai/`
- `prompts/` (non-agent-specific runtime prompt assets)

**Should avoid**
- Deterministic core rules in `packages/core/`
- DB schema ownership in `packages/db/`

**Expected deliverables**
- Prompt templates by interaction type.
- AI I/O mapping contracts aligned with shared types.

## 4) Database Agent
**Responsibilities**
- Define persistence model and data access boundaries.
- Ensure entities support session replay and reporting.

**Owns**
- `packages/db/`

**Should avoid**
- UI concerns in `apps/web/`
- Prompt and narration assets

**Expected deliverables**
- Initial schema definitions and repository interfaces.
- Data lifecycle documentation for sessions and scores.

## 5) Voice Interaction Agent
**Responsibilities**
- Define voice input/output interaction flow.
- Align transcript handling with event and response objects.

**Owns**
- Voice integration surfaces in `apps/web/`
- Voice-related adapters in `packages/ai/` (in collaboration with AI Prompting Agent)

**Should avoid**
- Core trigger logic in `packages/core/`
- Persistence ownership decisions in `packages/db/`

**Expected deliverables**
- Voice turn-state flow.
- Fallback behavior specs for tap/click response paths.

## 6) Scoring Agent
**Responsibilities**
- Define scoring rubric model and deterministic metric mapping.
- Connect evaluated responses to skill dimensions.

**Owns**
- Scoring modules in `packages/core/`
- Shared scoring contracts in `packages/shared/`

**Should avoid**
- UI implementation details
- Storage engine-specific implementations

**Expected deliverables**
- Session scoring pipeline.
- SkillScore aggregation definitions.

## 7) Product UX Agent
**Responsibilities**
- Define interaction patterns, pacing, and usability standards.
- Ensure clarity under mobile and voice-first constraints.

**Owns**
- UX documentation in `docs/`
- UI behavior guidance for `apps/web/`

**Should avoid**
- Core deterministic engine code.
- DB schema internals.

**Expected deliverables**
- UX flow maps and content tone guidelines.
- Acceptance criteria for major interaction steps.

## 8) QA / Test Planning Agent
**Responsibilities**
- Define test strategy across core, AI, UI, and persistence boundaries.
- Build coverage plan for deterministic and non-deterministic layers.

**Owns**
- Test planning docs in `docs/`
- Future test suites across owned tracks (in collaboration)

**Should avoid**
- Re-architecting ownership boundaries without architecture update.

**Expected deliverables**
- Layered test plan.
- Risk-based test matrix.
- Regression checklist for release gates.
