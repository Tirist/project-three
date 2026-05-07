# Architecture Overview

## Architectural Goal
Build a system where deterministic simulation logic controls the operational world state, while AI layers translate that state into natural interactions, narration, and coaching.

## Layered Structure

## 1) Application Layer (`apps/web`)
Responsibilities:
- Mobile-web user interaction.
- Voice-first capture and playback orchestration in the UI.
- Session start, in-day interaction, and debrief presentation.

Non-responsibilities:
- Deciding simulation outcomes.
- Defining prompt policy or core event rules.

## 2) Core Simulation Layer (`packages/core`)
Responsibilities:
- Canonical simulation state and transitions.
- Game-day clock progression and event triggering.
- Deterministic event resolution from player actions.
- Calculation of deterministic performance signals.

Non-responsibilities:
- Generating natural-language dialogue.
- Vendor-specific model interactions.

This layer is the source of truth.

## 3) AI Interaction Layer (`packages/ai`)
Responsibilities:
- Convert simulation state into dialogue/narration prompts.
- Produce in-character customer/staff communication style.
- Evaluate response quality against rubric context.
- Generate coaching summaries.

Non-responsibilities:
- Overriding deterministic simulation outcomes.
- Mutating core state outside approved interfaces.

This layer is expressive, not authoritative.

## 4) Data Layer (`packages/db`)
Responsibilities:
- Persist companies, configs, users, sessions, events, and scores.
- Provide storage interfaces for reads/writes.
- Support analytics and review workflows.

Non-responsibilities:
- UI composition.
- Prompt content ownership.

## 5) Shared Contracts (`packages/shared`)
Responsibilities:
- Shared type contracts and payload shapes.
- Cross-layer validation definitions.

## Core Architecture Rule
**Simulation engine owns truth.**
AI can explain, roleplay, and critique, but cannot invent authoritative state transitions.

## Typical Runtime Flow
1. User action enters through web client.
2. Core engine validates action against current state.
3. Core engine advances state and emits event/result payload.
4. AI layer receives structured context and produces narration/dialogue/evaluation text.
5. Web client presents updated state plus AI outputs.
6. Data layer persists session and scoring artifacts.

## Why This Split Matters
- Keeps behavior testable and reproducible.
- Prevents model variability from corrupting simulation fairness.
- Allows AI model changes without rewriting core rules.
- Supports future multimodal interfaces with stable backend logic.
