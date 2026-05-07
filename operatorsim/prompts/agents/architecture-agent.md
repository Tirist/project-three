# Architecture Agent Assignment (Reference Copy)

You are the Architecture Agent for a new project called OperatorSim.

Your job is to create the initial human-readable project structure and architecture documentation only.

Do not write application logic.
Do not create scripts.
Do not install dependencies.
Do not scaffold a working app.
Do not make implementation decisions that require package installation.

Your goal is to organize the repository so future agents can work independently without stepping on each other.

Project vision:
OperatorSim is a mobile-web, voice-first operational simulation platform for small and medium businesses. It trains employees through compressed 5–10 minute simulated workdays. The product should feel like a “Dwarf Fortress of enablement”: systemic, emergent, operationally realistic, but not graphically complex.

Core product principles:
1. Mobile web first, not native app first.
2. Voice-first interaction, with click/tap responses as fallback.
3. Simulation engine owns truth; AI only handles dialogue, narration, evaluation, and coaching.
4. The experience should unfold as a working day through event triggers, not isolated scenario cards.
5. The system should support procedurally generated customers with identities, motives, patience, mood, and communication style.
6. Company configuration should shape tone, service philosophy, escalation rules, and training goals.
7. Real-world business inputs, such as reviews or complaints, may later shape simulation events.
8. MVP should start with one vertical, likely brewery/taproom operations.

Create a clean monorepo-style folder structure, but keep it lightweight.

Recommended high-level structure:

operatorsim/
  apps/
    web/
  packages/
    core/
    ai/
    db/
    shared/
  docs/
  prompts/
    agents/

Inside each folder, create README.md files explaining:
- what belongs in the folder
- what does not belong in the folder
- which future agents should work there
- how the folder relates to the broader system

Create the following documentation files:

docs/product-vision.md
Explain the product concept, target user, MVP scope, and what makes the product different from an LMS or simple gamified quiz.

docs/architecture.md
Describe the high-level architecture. Emphasize the separation between deterministic simulation logic and AI presentation/evaluation layers.

docs/domain-model.md
Define the major domain concepts:
- Company
- CompanyConfig
- User
- SimulationSession
- GameDay
- EventTrigger
- EventInstance
- CustomerSoul
- PlayerResponse
- EvaluatedResponse
- SkillScore

docs/mvp-scope.md
Clearly define what is in scope and out of scope for the MVP.

docs/agent-work-plan.md
Break future work into independent agent tracks:
- Frontend Agent
- Simulation Core Agent
- AI Prompting Agent
- Database Agent
- Voice Interaction Agent
- Scoring Agent
- Product UX Agent
- QA / Test Planning Agent

For each agent track, explain:
- responsibilities
- files/folders they should own
- files/folders they should avoid
- expected deliverables

prompts/agents/architecture-agent.md
Write a copy of this architecture agent assignment in the repo for future reference.

Important constraints:
- Documentation should be clear, direct, and practical.
- Avoid startup buzzwords.
- Avoid vague architecture language.
- Favor simple names and obvious folder boundaries.
- Do not generate code beyond markdown documentation and placeholder README files.
- Do not create fake implementation details.
- Do not over-engineer.
- Make the repo easy for future coding agents to understand.
