# Ootils — Outside-In Market & GTM Assessment

**Date:** April 2026  
**Prepared for:** Nico (Founder, Ootils)  
**Analyst:** Strategic product/GTM review (external perspective)

---

## Executive Summary

Ootils is targeting a real gap in a market that is structurally ready for disruption. The timing is credible. The architecture is genuinely differentiated — not just positioning. The "AI agent-first infrastructure" framing is correct and early, which is both the biggest asset and the biggest risk. The GTM path is narrow but navigable. The existential risk is not competition — it's distribution: getting to the right first customer before runway runs out or the window narrows.

---

## 1. Market Timing

### Verdict: Window is open. Not wide, but real. 2025–2027 is the entry window.

**Tailwinds:**

- **LLM agents are moving from demos to operations.** In 2024, AI in supply chain was mostly "copilot" dashboards. In 2025–2026, the leading edge — logistics tech, embedded software teams, digital-native manufacturers — is experimenting with autonomous agents running planning logic. Ootils is designed for exactly this layer.
- **APS incumbents are architecturally frozen.** Kinaxis, IBP, Blue Yonder were built for human planners operating in weekly/monthly cycles. Their data models are tabular, not graph; their explainability is post-hoc (reports), not structural. Retrofitting graph-native AI agent orchestration onto these stacks is not an 18-month project — it's a re-architecture.
- **Supply chain as a strategic function has elevated buyers.** COVID + tariff shocks + nearshoring pressure have elevated supply chain from ops cost center to C-suite priority. Budget has followed. CISOs, CTOs, and COOs are now involved in planning stack decisions that used to be owned by IT.
- **Python/API-first stack expectations are mainstream even in enterprise.** dbt normalized the idea of ELT as developer infrastructure. Airbyte normalized data connectors. The market is primed to accept supply chain planning as composable infrastructure — especially among companies that already have modern data stacks.
- **Tariff volatility (2025 context)** is forcing companies to run more scenarios, faster, and across more variables than traditional APS can handle. The scenario simulation use case has become acutely painful in real-time.

**Headwinds:**

- **Enterprise buyers are still risk-averse on "AI planning."** The narrative of "AI running your supply chain autonomously" creates fear, not excitement, in traditional ops teams. Positioning matters enormously here.
- **The market is crowded with noise.** Every APS vendor is shipping "AI-powered" features. Every new SCM startup says "AI-native." The signal-to-noise ratio is terrible, and buyers are fatigued by pitches.
- **Infrastructure without a UI is a hard sell to supply chain operators.** The buyer persona that approves budget (VP Supply Chain, COO) is not usually a developer. The decision-maker and the technical champion are different people and need different narratives.
- **Proof of scale is a prerequisite.** Going from 100 SKUs × 5 locations to 10,000 SKUs × 50 locations is not a tweak. Enterprise credibility requires demonstrated scale before meaningful deals.

**Timing summary:** The window opened roughly Q3 2024 and will remain meaningfully open through 2027. After that, either the incumbents will have shipped credible AI agent platforms (unlikely but possible), or a VC-backed competitor with more resources will have captured the market (more likely). Two years of focused execution is the realistic window to establish a beachhead.

---

## 2. Competitive Landscape

### Tier 1: Legacy APS (Kinaxis, SAP IBP, Blue Yonder, o9 Solutions)

**Real threat level: Medium-Low in year 1, Medium-High by year 3.**

These vendors have the customer base, the integrations, and the enterprise trust. They are all shipping AI features aggressively. But their architecture is a liability:

- Kinaxis (RapidResponse) is a proprietary concurrent planning model — not graph-native, not agent-first. Their "AI" layer is bolt-on.
- SAP IBP runs on HANA — deeply tabular, highly coupled to ERP. Structural AI agent integration is genuinely hard.
- o9 Solutions has a knowledge graph layer but it's not event-sourced or agent-orchestrated.
- Blue Yonder acquired Yantriks and is pushing "luminate" AI — very UI-heavy, not infrastructure.

**Conclusion:** The incumbents are not building what Ootils is building. They are building AI-assisted planners, not agent-executable planning infrastructure.

### Tier 2: New Entrants / Adjacent Movers (Watch Closely)

- **Relex Solutions:** Retail/CPG focus. Excellent demand forecasting, expanding into supply planning. Less relevant to discrete manufacturing but moving fast in adjacent space.
- **Coupa (Supply Chain Design):** Post-acquisition of LLamasoft — they have network design / scenario modeling capability but it's strategic planning, not operational. Different use case.
- **Optilogic:** Cloud-native supply chain design. Solver-based, not agent-based. Limited operational layer.
- **Pando:** Logistics/fulfillment focus. Not a planning engine.
- **Nulogy / Flexe / project44:** All adjacent but not directly competing.
- **Agentic AI horizontal players (Langchain, CrewAI, Autogen ecosystems):** These could enable a competitor to build a supply chain agent layer on top of an existing APS. This is the stealth risk — not a supply chain startup, but an AI company that builds a planning agent as a vertical product. Watch for VC-backed "AI supply chain" plays coming out of YC, a16z, or Bessemer in 2025–2026.
- **SAP / Microsoft partnership:** Microsoft is pushing Copilot deeply into Dynamics 365 Supply Chain. If they build a graph-based planning substrate under Copilot agents, that would be a direct threat — but the org inertia makes it unlikely to be architecturally clean within 2 years.
- **Palantir AIP in manufacturing:** Palantir is explicitly positioning AIP as an agent-orchestration layer for operations. Their supply chain module is nascent but they have the architecture and the enterprise relationships. This is the most dangerous adjacent player for mid-to-large enterprise.

### Stealth Risk: Databricks / Snowflake / dbt + supply chain agent layer

A "supply chain agent" built on top of a Databricks lakehouse with a dbt-modeled supply chain semantic layer and LLM agents is not a crazy thing. It could emerge from a consulting firm (Slalom, Accenture's AI units), a data engineering startup, or even a VC-backed play. It's not there yet, but it's conceivable in 18 months if the market signals are strong.

---

## 3. Differentiation Reality Check

### Claim: "Graph-native + AI agent-first + native explainability"

**Brutal assessment: Genuinely differentiated today. Replicable in 3–5 years, not 18 months. But only differentiated if you actually deliver it — not just claim it.**

**Graph-native:**  
The supply chain as a typed-node/typed-edge graph is architecturally correct and genuinely rare. Traditional APS systems are built on time-phased buckets and relational tables. Graph gives you structural propagation, causal paths, and relationship-aware simulation that tabular systems cannot replicate without a full rewrite. The incumbents know this — Kinaxis has talked about graph models internally for years. But shipping it cleanly, with event-sourcing and incremental propagation, is a multi-year rewrite project for an organization with thousands of enterprise customers to support. This moat is real but time-limited.

**AI agent-first:**  
This is the most defensible differentiator if executed correctly. "Designed for agents" is not just a marketing claim — it means the API surface, the data model, the event semantics, and the explainability layer are all built with LLM agent orchestration as the primary interaction paradigm. No incumbent has built this from scratch. They have bolted agents on top of existing UI-first products. The structural difference matters for: latency, context window efficiency, action grounding, and error recovery. However: to prove this differentiation, you need an actual agent doing non-trivial planning work on real data. A demo of an agent calling /explain and /simulate is not enough. You need to show it operating autonomously over time.

**Native explainability:**  
This is underrated. In enterprise supply chain, "why did the system recommend this?" is not a nice-to-have — it's a compliance, audit, and change management requirement. Traditional APS explainability is report-based (look at this table after the fact). Structural causal paths in the graph (demand node → supply node → constraint → root cause) are a fundamentally better answer. This differentiator resonates immediately with experienced supply chain planners who have been burned by black-box recommendations.

**What incumbents can replicate:**  
- A graph visualization layer on top of existing data: 6–12 months  
- An "AI assistant" that answers questions: already shipped by most  
- Native structural explainability with causal paths: 3–5 years (requires data model change)  
- True agent-first API with event-sourced propagation: 3–5 years (requires architecture change)

**What they cannot replicate easily:**  
- Technical credibility with developer-first buyers who can read an API spec  
- Speed of iteration and absence of legacy constraints  
- A founder with 20+ years operational depth who can speak to both the technical and the operational buyer

**Conclusion:** The differentiation is real, but it must be demonstrated, not just stated. "Graph-native and AI agent-first" said by a startup means nothing without a working system that an expert can interrogate.

---

## 4. Target Customer

### The theoretical TAM is irrelevant. The real first customer is this person:

**Profile:**
- **Title:** VP/Director of Supply Chain IT, or Head of Supply Chain Transformation, or sometimes VP/Director of Operations Technology  
- **Company type:** $200M–$2B revenue manufacturer or distributor. Discrete manufacturing (industrial, electronics, medical devices) or complex distribution. NOT retail/CPG (different planning paradigm). NOT Fortune 50 (procurement cycles too long, too political).
- **Situation:** Has already invested in a modern data stack (Snowflake, Databricks, or similar). Has already started an AI initiative. Is frustrated that their APS (likely SAP IBP, Kinaxis, or a legacy system) cannot expose planning logic to agents. Has a technical team — either internal data engineers or a trusted SI partner.
- **Pain:** They are running planning in Excel as a workaround because the APS is too rigid. Or they are manually triaging exceptions every morning. Or they've tried to build AI agents but have no structured substrate to connect them to planning logic.
- **Budget:** $150K–$500K/year is realistic for a pilot-to-production path. This person controls or influences this budget.
- **Buying process:** Technical champion (data engineer, architect) evaluates and pilots. Business buyer (VP Supply Chain, COO) approves. Procurement is involved but not the driver. Sales cycle: 3–6 months for a pilot, 6–12 months for a production contract. POC is the unlock.

**The one-line ICP:** *A technically sophisticated supply chain team at a mid-market manufacturer who has already modernized their data stack and is now blocked because their planning system can't talk to their AI agents.*

**Why this is the right target:**  
- They have the pain AND the technical vocabulary to evaluate Ootils  
- They are not trying to rip and replace Kinaxis — they want to augment or replace a legacy/inadequate planning layer  
- They have budget and an internal champion who can navigate procurement  
- They are small enough that Ootils can win without an army of enterprise sales reps

**Anti-target:**  
- A company running Kinaxis or IBP successfully and happy with it (no pain, no urgency)  
- A company with no data infrastructure (can't plug in Ootils)  
- A Fortune 500 running a formal RFP (too slow, too political, too risky for a pre-revenue company)

---

## 5. GTM Direction

### Verdict: Bottom-up technical sale → founder-led direct → selective partner leverage

**Not developer-led in the dbt/Airbyte sense.** Supply chain planning is not bought by individual developers. There is no "free tier → team plan" motion here. The buyer is a supply chain executive who approves a budget line.

**Not enterprise sales.** Not at this stage. No SDR/AE motion. No Salesforce CRM with 200 leads. Too expensive, too slow, wrong for a pre-revenue product.

**The right motion: Founder-led, POC-first, reference customer strategy.**

1. **Stage 1 (now → first paying customer):** Nico sells personally to 1–2 companies that fit the ICP exactly. The pitch is a working technical demo on real-ish data. The goal is a paid pilot ($30K–$80K), not a production contract. This validates commercial viability and generates a reference customer.

2. **Stage 2 (first customer → 3–5 customers):** Nico expands via warm networks (supply chain executive community, former colleagues, LinkedIn presence in AI + supply chain). Targets digital operations leaders who are already publishing about AI in supply chain. Content plays a role — not thought leadership fluff, but specific technical/operational insights (e.g., "How graph-native propagation handles multi-echelon shortage attribution differently than MRP").

3. **Stage 3 (5+ customers):** Selective SI partnership with a boutique that specializes in supply chain transformation. Avoid the Big 4 initially — they will stall the motion. Target a 30–100 person supply chain tech consultancy that wants to differentiate its practice with AI-native planning.

**The killer entry point:**  
*"Your AI agents can't run planning operations because your planning system wasn't built for them. Ootils is the planning substrate your agents can actually use."*  
This lands with the technical buyer immediately. The follow-up for the business buyer: *"Every shortage your team triages manually is a structured event in Ootils with a causal path and a simulation capability. Your planners stop firefighting and start supervising."*

**What to avoid:**  
- A product-led growth motion (no self-serve path makes sense yet)  
- Conference-first awareness strategy (expensive, long conversion cycles)  
- Broad "supply chain AI" positioning (too noisy, no differentiation)

---

## 6. Risks

### Risk 1: Distribution before product-market fit evidence

The single biggest commercial risk is not competition — it's that Ootils never gets in front of the right first buyer. With no customers, no case study, no reference, and no GTM motion yet, the risk is spending 12–18 months building a technically excellent product that no one has validated will be purchased. The founding team has zero sales infrastructure, no warm pipeline, and the target buyer (supply chain transformation leader at a mid-market manufacturer) is not searching for "AI planning engine" on Product Hunt. Getting to the first paid POC is a distribution problem, not a product problem.

**Mitigation:** Nico should run an immediate "fake door" commercial test — identify 5–10 ICP companies, reach out personally, run a conversation, and see if the POC narrative lands before the architecture is fully proven. The architecture validation and the commercial validation should happen in parallel.

### Risk 2: The "infrastructure" positioning doesn't resonate with buyers who need a solution

"Infrastructure" is a compelling framing for a technical audience. But supply chain buyers — even technically sophisticated ones — buy solutions to problems, not infrastructure. "We built a planning engine that AI agents can run" is not a sentence that gets budget approved. The risk is that Ootils is positioned so far upstream that no one understands what problem it solves on Monday morning. The infrastructure framing is correct architecturally and strategically, but it needs a business outcome translation layer. This is a messaging risk that has killed more than one technically excellent product.

**Mitigation:** Develop two parallel narratives — the infrastructure framing for technical champions (architects, data engineers, CTO) and an outcome framing for business buyers ("your planners will spend 80% less time triaging exceptions because the system does it autonomously"). Test both. Lead with the outcome in cold outreach, earn the right to go deep on architecture.

### Risk 3: A well-funded competitor captures the ICP before Ootils has references

The supply chain AI space is attracting VC capital. A startup with $10M raised, 10 sales reps, and a technically inferior but better-packaged product can capture the early-adopter mid-market faster than a technically superior product with one founder and no GTM. The YC/a16z pipeline in logistics/supply chain tech is active. If a competitor gets to 5–10 reference customers in the ICP segment in the next 12 months, they establish the category narrative and Ootils becomes a "also ran" regardless of technical superiority.

**Mitigation:** Speed to first reference customer is the only answer. Being 6 months earlier with a paid reference is worth more than being 12 months later with a better product. This means accepting some technical incompleteness in favor of commercial progress.

---

## 7. Verdict

### Is this a real opportunity? Yes.

The gap Ootils is targeting — a planning substrate designed for AI agent orchestration, with graph-native propagation and structural explainability — is real, underserved, and architecturally correct. The incumbents cannot quickly replicate it. The timing is early but not too early. The founder's domain expertise is a genuine asset.

### Is the direction right? Mostly yes, with one correction needed.

The architecture direction is right. The "AI agent-first infrastructure" thesis is right. The decision to not try to replace Kinaxis for Fortune 500 is right.

The one correction: **The "infrastructure" positioning needs a business outcome translation layer immediately.** Not later — now. Because the first commercial conversations will fail if the buyer can't connect "graph-native planning engine" to a budget line item they already have.

### What to double down on:

1. **The explainability story.** This is the most immediately understandable differentiator. "Every shortage has a causal path from demand to constraint" is a sentence that a VP Supply Chain who has been burned by black-box APS recommendations will react to viscerally. Lead with this.

2. **The scenario simulation capability.** With tariff volatility and nearshoring decisions live in 2025–2026, "cheap what-if analysis at the planning layer" is an acute pain. Delta overlays, not full copies — this is technically elegant AND commercially relevant right now.

3. **The API-first, open architecture posture.** This is the flag that signals "we are not going to lock you in like Kinaxis did." Plant it early and consistently.

### What to change:

1. **Start the commercial validation motion now, in parallel with architecture proof.** Don't wait until the proof-of-architecture is complete to start founder-led sales conversations. The risk of "perfect product, no customers" is higher than "slightly rough product, paying pilot customer."

2. **Identify and pursue 3 specific target companies** that fit the ICP criteria above. Not a list of 50. Three. With specific hypothesis for why each one has the pain, the technical maturity, and a decision-maker accessible to Nico's network.

3. **Build the "outcome narrative" now.** Write the one-pager that a VP Supply Chain can read and immediately say "this is my problem." Architecture docs come after. Business case first.

### Final word:

The product direction is correct. The market is real. The architecture is genuinely differentiated. The risk is purely commercial — getting distribution, getting to the first paying customer, and surviving long enough to build the reference architecture that makes the next deal easier. That is the only problem to solve right now.

---

*Assessment based on market knowledge as of Q1–Q2 2026. Competitive landscape evolves rapidly; reassess quarterly.*
