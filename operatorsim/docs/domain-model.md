# Domain Model

This document defines major domain concepts and their roles.

## Company
Represents a business tenant using OperatorSim.
- Owns users, configurations, and training sessions.
- Defines organizational context for all simulations.

## CompanyConfig
Represents company-specific operating philosophy and training constraints.
Includes:
- Tone and service style preferences.
- Escalation rules.
- Priority skills and coaching focus.
- Vertical-specific settings (MVP: brewery/taproom).

## User
Represents a person in the system.
Common roles:
- Trainee (runs simulations).
- Manager/trainer (reviews outcomes, adjusts config).

## SimulationSession
Represents one playable training run by one user.
- Tied to one Company and one CompanyConfig snapshot.
- Contains one GameDay timeline.
- Stores responses, outcomes, and summary scoring.

## GameDay
Represents the compressed operational day.
- Has a start state, progression timeline, and end state.
- Contains trigger windows where events may occur.

## EventTrigger
Represents deterministic conditions that can fire events.
Examples:
- Time-based trigger windows.
- Queue/load thresholds.
- Follow-on consequences from prior decisions.

## EventInstance
Represents a specific event fired during a session.
- Includes event context and affected entities.
- Tracks whether it is active, resolved, escalated, or expired.

## CustomerSoul
Represents a procedurally generated customer identity model.
Attributes can include:
- Motive and urgency.
- Patience profile.
- Mood baseline and volatility.
- Communication style.

CustomerSoul influences interaction behavior across related events.

## PlayerResponse
Represents the trainee's input for an event.
- Voice transcript and/or selected fallback actions.
- Timestamp and event linkage.

## EvaluatedResponse
Represents structured interpretation of PlayerResponse.
- Evaluation dimensions (clarity, policy fit, empathy, timing, etc.).
- Notes linking feedback to event context.

AI may assist generation, but schema remains deterministic.

## SkillScore
Represents tracked performance metrics.
- Per-session metric values.
- Aggregated trend values across sessions.
- Skill domains aligned to CompanyConfig goals.
