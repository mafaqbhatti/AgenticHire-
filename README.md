<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=4f46e5&height=220&section=header&text=AgenticHire&fontSize=70&fontColor=ffffff&fontAlignY=35&desc=Autonomous%20Multi-Agent%20AI%20Recruitment%20%26%20Interview%20Pipeline&descAlignY=55&descSize=19&descColor=c7d2fe" width="100%"/>

<br/>

[![Node.js](https://img.shields.io/badge/Node.js-20+-339933?style=for-the-badge&logo=node.js&logoColor=white)](https://nodejs.org)
[![Next.js](https://img.shields.io/badge/Next.js-14-000000?style=for-the-badge&logo=next.js&logoColor=white)](https://nextjs.org)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://typescriptlang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Socket.io](https://img.shields.io/badge/Socket.io-4-010101?style=for-the-badge&logo=socket.io&logoColor=white)](https://socket.io)
[![Groq AI](https://img.shields.io/badge/Groq_AI-LLaMA_3_70B-F55036?style=for-the-badge&logo=groq&logoColor=white)](https://groq.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)

<br/>

> **AgenticHire** compresses a 2-to-4 week recruitment cycle into a fully autonomous **3-minute AI pipeline.**  
> Five specialized autonomous agents screen resumes, generate tailored technical assessments, grade submissions, conduct live voice interviews, and synthesize final executive hiring recommendations—keeping humans in the loop only where high-leverage judgment matters.

<br/>

```
  Resume Submitted  ──►  Agent 1: Screen  ──►  Agent 2: Test  ──►  Agent 3: Grade  ──►  Agent 4: Interview  ──►  Agent 5: Synthesize  ──►  HR Decision
        📄                     🤖                   📝                  📊                     🎤                          🏁                   ✅
```

</div>

---

## 📌 Table of Contents

- [Why AgenticHire?](#-why-agentichire)
- [System Architecture](#-system-architecture)
- [The 5-Agent Autonomous Swarm](#-the-5-agent-autonomous-swarm)
- [Dual-Frontend Architecture (Next.js + Streamlit)](#-dual-frontend-architecture)
- [Real-Time Infrastructure](#-real-time-infrastructure)
- [Security & Isolation Model](#-security--isolation-model)
- [Tech Stack Deep Dive](#-tech-stack-deep-dive)
- [Project Directory Structure](#-project-directory-structure)
- [Getting Started](#-getting-started)
- [Environment Configuration](#-environment-configuration)
- [REST API Reference](#-rest-api-reference)
- [End-to-End Workflow](#-end-to-end-workflow)

---

## 🎯 Why AgenticHire?

Traditional technical recruiting is plagued by operational latency. Job postings yield hundreds of applications, requiring HR teams to manually parse resumes, craft take-home tasks, score candidate responses, schedule exploratory calls, and conduct initial phone screens.

**AgenticHire** automates repetitive evaluation rounds with deterministic, bias-mitigated, multi-agent AI execution:

| Stage | Traditional Hiring | AgenticHire Pipeline |
|---|---|---|
| **Resume Screening** | 2–4 days | **~15 seconds** (Agent 1) |
| **Assessment Design** | 1–2 days | **Instant dynamic generation** (Agent 2) |
| **Answer Evaluation** | 1–3 days | **~25 seconds structured grading** (Agent 3) |
| **First-Round Interview** | 3–5 days scheduling | **On-demand AI voice interview** (Agent 4) |
| **Comprehensive Report** | 1–2 days compilation | **~10 seconds holistic synthesis** (Agent 5) |
| **Total Turnaround** | **2 to 4 Weeks** | **~3 Minutes Active Processing** |

---

## 🏗 System Architecture

AgenticHire is structured as an **event-driven, decoupled multi-service architecture**. Every layer scales independently, and asynchronous message passing ensures zero task loss even during node restarts.

```mermaid
flowchart TD
    subgraph Clients["Presentation Layer"]
        NextUI["Next.js 14 App Router\n(Interactive Kanban & Voice Room)"]
        StreamlitUI["Streamlit Dashboard\n(Executive Analytics & Candidate Review)"]
    end

    subgraph Gateway["Backend & Gateway Layer"]
        Server["Node.js 20 / Express.js REST API"]
        SocketServer["Socket.io Server\n(Tenant-Scoped Real-time Rooms)"]
        AuthMiddleware["JWT Authentication & RBAC"]
    end

    subgraph StateStorage["State & Event Streaming Layer"]
        PG[("PostgreSQL 16\n(Prisma ORM Multi-tenant)")]
        RedisCache[("Redis 7\n(Sub-ms Stage Cache & Room Tokens)")]
        KafkaBus["Apache Kafka 7.5\n(Event Streaming Backbone)"]
    end

    subgraph AgentSwarm["Autonomous Agent Swarm (Groq LLaMA 3 70B)"]
        A1["Agent 1: Resume Screener"]
        A2["Agent 2: Assessment Architect"]
        A3["Agent 3: Answer Evaluator"]
        A4["Agent 4: Voice Interview Coordinator"]
        A5["Agent 5: Executive Synthesizer"]
    end

    Clients -->|"HTTP REST & WebSockets"| Gateway
    Gateway --> PG
    Gateway --> RedisCache
    Gateway -->|"Publish Event"| KafkaBus

    KafkaBus -->|"resume-submitted"| A1
    A1 -->|"screening-passed"| KafkaBus
    KafkaBus -->|"screening-passed"| A2
    A2 -->|"assessment-submitted"| KafkaBus
    KafkaBus -->|"assessment-submitted"| A3
    A3 -->|"interview-invited"| KafkaBus
    KafkaBus -->|"interview-invited"| A4
    A4 -->|"interview-completed"| KafkaBus
    KafkaBus -->|"interview-completed"| A5

    AgentSwarm -.->|"Sync Progress"| SocketServer
    AgentSwarm -.->|"Persist State"| PG
    AgentSwarm -.->|"Fast Status Cache"| RedisCache
```

### Architectural Highlights

* **Decoupled Kafka Message Bus**: Agents do not directly invoke each other. If downstream workers restart or experience backpressure, Kafka queues events durably with zero data loss.
* **Redis Speed Layer**: High-frequency dashboard Kanban polling is served via Redis with sub-millisecond response latency, leaving PostgreSQL unburdened.
* **Structural Multi-Tenancy**: Every entity is scoped by `tenantId`. Data segregation is enforced at the database query layer, ensuring total isolation between organizations.

---

## 🤖 The 5-Agent Autonomous Swarm

Each agent functions as an independent, deterministic worker consuming events from Kafka, querying **Groq LLaMA 3 70B** with strict JSON schemas, persisting state in PostgreSQL, and emitting live Socket.io events.

```
                         ┌─────────────────────────────────┐
                         │         KAFKA EVENT BUS         │
                         │                                 │
  resume-submitted ──────►  Agent 1: Resume Screener       │
                         │     • Groq LLaMA 3 70B          │
                         │     • Score 0–100 vs Job Spec   │
                         │     • Qualified if score ≥ 60   │
                         │             │                   │
                         │    screening-passed ────────────►
                         │                                 │
                         │  Agent 2: Assessment Generator  │
                         │     • Role-specific challenges  │
                         │     • Dynamic question sets     │
                         │     • Automated SMTP dispatch   │
                         │             │                   │
                         │   assessment-submitted ─────────►
                         │                                 │
                         │  Agent 3: Answer Evaluator      │
                         │     • Per-question rubrics      │
                         │     • Multi-criteria scoring    │
                         │     • Shortlist if score ≥ 50   │
                         │             │                   │
                         │    interview-invited ───────────►
                         │                                 │
                         │  Agent 4: Interview Coordinator │
                         │     • Generates 24h JWT token   │
                         │     • Web Speech TTS & STT      │
                         │     • Live timed voice room     │
                         │             │                   │
                         │  interview-completed ───────────►
                         │                                 │
                         │  Agent 5: Executive Synthesizer │
                         │     • Multi-stage weighted calc │
                         │     • Strengths vs. gaps report │
                         │     • Final HIRE / REJECT rec   │
                         └─────────────────────────────────┘
```

### 1. Agent 1 — Resume Screener
* **Trigger**: Candidate uploads resume PDF via `/apply/:jobId`.
* **Execution**: Resumes are decrypted in-memory (AES-256), text is extracted via `pdf-parse`, and LLaMA-3 evaluates applicant credentials against role requirements.
* **Decision Gate**: `score >= 60` $\rightarrow$ Emits `screening-passed`. Otherwise marked as `SCREENED_OUT`.

### 2. Agent 2 — Assessment Architect
* **Trigger**: Receives `screening-passed` event.
* **Execution**: Synthesizes 5 rigorous, role-tailored technical questions testing real-world architectural design, edge-case mitigation, and problem-solving.
* **Output**: Issues a unique single-use assessment UUID token and dispatches a magic invite link via Nodemailer.

### 3. Agent 3 — Answer Evaluator
* **Trigger**: Candidate submits responses via `/assessment/:token/submit`.
* **Execution**: Scores every response independently with granular feedback, evaluating technical depth, practical accuracy, and clarity.
* **Decision Gate**: `totalScore >= 50` or `recommendation === 'SHORTLIST'` $\rightarrow$ Emits `interview-invited`.

### 4. Agent 4 — Voice Interview Coordinator
* **Trigger**: Receives `interview-invited` event.
* **Execution**: Generates 4 tailored situational questions and provisions a cryptographically signed 24h JWT room token stored in Redis.
* **Voice Room (`/interview/:token`)**:
  * Native browser text-to-speech (`SpeechSynthesisUtterance`) reads questions aloud.
  * Real-time speech recognition (`webkitSpeechRecognition`) transcribes spoken candidate answers with a live 60-second response window.

### 5. Agent 5 — Executive Recommendation Synthesizer
* **Trigger**: Receives `interview-completed` event with session transcript.
* **Execution**: Computes a multi-stage weighted aggregate score across screening, assessments, and interview performance. Generates an executive hiring brief with strengths, flags, and an actionable decision recommendation (`HIRE`, `STRONG_HIRE`, `CONSIDER`, or `REJECT`).

---

## 💻 Dual-Frontend Architecture

AgenticHire provides two presentation experiences tailored to different team workflows:

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│       Next.js 14 Application         │     │         Streamlit Dashboard          │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ • Production HR Management Portal    │     │ • Executive Analytics & Reporting    │
│ • Live Drag-and-Drop Kanban Board    │     │ • Candidate Deep-Dive & Scoring View │
│ • Candidate Voice Interview Room     │     │ • Lightweight Python Operations      │
│ • Public Application Portals         │     │ • One-Click Zero-Config Cloud Deploy │
└──────────────────────────────────────┘     └──────────────────────────────────────┘
```

* **Next.js 14 Web Portal**: App Router, Tailwind CSS, TypeScript, Socket.io real-time Kanban cards, and Web Speech voice interview portal.
* **Streamlit Executive Dashboard**: Built with Python & Streamlit, providing rapid data inspection, batch candidate filtering, scoring distributions, and zero-overhead deployments.

---

## ⚡ Real-Time Infrastructure

* **Socket.io Multi-Tenant Channels**: HR dashboards join isolated rooms (`tenant:<tenantId>`). Pipeline events immediately transition Kanban cards with zero page reloads.
* **Distributed Kafka Workers**: Decoupled producers and consumer groups handle spikes in applicant traffic without dropping jobs.
* **Redis In-Memory State**: Candidate stages, interview verification tokens, and job metadata are cached with automatic TTL eviction.

---

## 🔐 Security & Isolation Model

* **AES-256-CBC Resume Encryption**: Resumes are encrypted on arrival with an initialization vector (IV) prepended to the ciphertext. File decryption occurs strictly in-memory during screening.
* **Dual-Layer Interview Verification**: Interview room access requires a valid HS256-signed JWT token matching an active key stored in Redis.
* **Multi-Tenant Boundary**: All SQL queries via Prisma ORM enforce mandatory `tenantId` parameter scoping.
* **Zero Secret Leakage**: Strict `.gitignore` protections ensure credentials, local certificates, and test candidate files are never committed.

---

## 🛠 Tech Stack Deep Dive

| Component | Technology | Rationale |
|---|---|---|
| **Primary Frontend** | Next.js 14 (React 18, TypeScript) | Server-side rendering, App Router, high-speed UX |
| **Analytics Dashboard** | Python 3.10+ & Streamlit | Rapid interactive data exploration & metrics |
| **Backend API** | Node.js 20 & Express.js | High-throughput asynchronous event processing |
| **ORM & Relational DB** | Prisma ORM & PostgreSQL 16 | ACID-compliant state storage with schema safety |
| **Cache & Ephemeral Tokens** | Redis 7 (ioredis) | Sub-millisecond stage queries & expiring sessions |
| **Event Streaming** | Apache Kafka 7.5 (KafkaJS) | Resilient, distributed asynchronous agent handoffs |
| **Real-time WebSockets** | Socket.io 4 | Instant live updates across HR Kanban views |
| **AI Inference** | Groq Cloud SDK (LLaMA 3 70B) | Sub-second inference latency with JSON-mode output |
| **Containerization** | Docker & Docker Compose | Uniform local & staging infrastructure orchestration |

---

## 📁 Project Directory Structure

```
AgenticHire/
├── 📄 docker-compose.yml          # PostgreSQL 16, Redis 7, Kafka 7.5, Zookeeper
├── 📄 .env.example                # Unified configuration template
├── 📄 README.md                   # System documentation
│
├── 📂 backend/                    # Core API & Agent Engine
│   ├── 📄 server.js               # Express & Socket.io entry point
│   ├── 📄 package.json
│   ├── 📂 prisma/
│   │   └── 📄 schema.prisma       # Database schema (Tenants, Jobs, Candidates, Evals)
│   ├── 📂 src/
│   │   ├── 📂 agents/             # 5 Autonomous AI Agent Implementations
│   │   │   ├── 📄 agent1.screener.js
│   │   │   ├── 📄 agent2.assessor.js
│   │   │   ├── 📄 agent3.evaluator.js
│   │   │   ├── 📄 agent4.interviewer.js
│   │   │   ├── 📄 agent5.evaluator_final.js
│   │   │   └── 📄 prompts.js      # Structured system prompts
│   │   ├── 📂 kafka/              # Kafka producer, consumer, and topic definitions
│   │   ├── 📂 routes/             # REST endpoints (auth, jobs, candidates, assessments)
│   │   ├── 📂 sockets/            # Real-time WebSocket server and room broadcast
│   │   └── 📂 services/           # Groq AI, Redis, Nodemailer, AES-256 crypto
│   └── 📂 uploads/                # Encrypted resume store
│
├── 📂 frontend/                   # Next.js 14 Web Portal
│   ├── 📄 package.json
│   └── 📂 src/
│       ├── 📂 app/                # App Router (dashboard, jobs, candidates, interview room)
│       ├── 📂 hooks/              # Socket & authentication hooks
│       └── 📂 lib/                # API client & socket singletons
│
└── 📂 streamlit_app/              # Python Streamlit Executive Dashboard
    ├── 📄 app.py                  # Streamlit entry point
    ├── 📄 requirements.txt        # Python dependencies
    ├── 📂 pages/                  # Dashboard, Jobs, Candidates, Public Views
    └── 📂 utils/                  # Auth, API client, and layout helpers
```

---

## 🚀 Getting Started

### Prerequisites

Ensure you have installed:
* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* [Node.js v20+ LTS](https://nodejs.org)
* [Python 3.10+](https://python.org)
* [Groq Cloud API Key](https://console.groq.com) *(Free tier available)*

---

### Step 1: Clone Repository

```bash
git clone https://github.com/mafaqbhatti/AgenticHire-.git
cd AgenticHire-
```

### Step 2: Spin Up Infrastructure

Boot up PostgreSQL, Redis, Kafka, and Zookeeper in detached mode:

```bash
docker-compose up -d
```

Verify services are healthy:
```bash
docker ps
```

### Step 3: Configure Environment Variables

```bash
cp .env.example backend/.env
```

Open `backend/.env` and insert your **Groq API Key**:
```env
GROQ_API_KEY="gsk_your_groq_api_key_here"
```

### Step 4: Initialize & Run Backend

```bash
cd backend
npm install
npx prisma generate
npx prisma db push
npm run dev
```
*Backend initializes on `http://localhost:5000` with Kafka consumer groups and Socket.io.*

### Step 5: Launch Next.js Web Portal

In a new terminal:
```bash
cd frontend
npm install
npm run dev
```
*Web client is live at `http://localhost:3000`.*

### Step 6 (Optional): Launch Streamlit Analytics Dashboard

In another terminal:
```bash
cd streamlit_app
python -m venv venv

# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate

pip install -r requirements.txt
streamlit run app.py
```
*Streamlit dashboard runs at `http://localhost:8501`.*

---

## 🔑 Environment Configuration

```env
# ── Database (PostgreSQL 16) ──────────────────────────────────
DATABASE_URL="postgresql://admin:hireflow_secret@localhost:5432/hireflow"

# ── Redis Cache & Session Store ──────────────────────────────
REDIS_URL="redis://localhost:6379"

# ── Event Streaming (Apache Kafka) ───────────────────────────
KAFKA_BROKER="localhost:9092"

# ── Authentication ───────────────────────────────────────────
JWT_SECRET="your_64_char_secure_random_key_here"
JWT_EXPIRES_IN="7d"

# ── Groq LLaMA 3 70B AI Engine ──────────────────────────────
GROQ_API_KEY="gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# ── Email Dispatch (SMTP) ───────────────────────────────────
EMAIL_HOST="smtp.gmail.com"
EMAIL_PORT=587
EMAIL_USER="your-email@gmail.com"
EMAIL_PASS="your_16_digit_app_password"
EMAIL_FROM="AgenticHire <noreply@agentichire.ai>"

# ── Security & Cryptography ─────────────────────────────────
ENCRYPTION_KEY="32_character_crypto_secret_key!!"

# ── Network & Ports ─────────────────────────────────────────
PORT=5000
NODE_ENV=development
FRONTEND_URL="http://localhost:3000"
```

---

## 🌐 REST API Reference

All protected endpoints require an `Authorization: Bearer <JWT>` header.

| Method | Endpoint | Access | Function |
|---|---|---|---|
| `POST` | `/api/auth/register` | Public | Register enterprise tenant & admin account |
| `POST` | `/api/auth/login` | Public | Authenticate user & return JWT token |
| `GET` | `/api/auth/me` | Protected | Fetch authenticated tenant context |
| `GET` | `/api/jobs` | Protected | List all company job postings |
| `POST` | `/api/jobs` | Protected | Create new position with requirements |
| `GET` | `/api/jobs/:id` | Protected | Position details & candidate stages |
| `GET` | `/api/jobs/public/:id` | Public | Public position information for candidates |
| `POST` | `/api/candidates/apply` | Public | Candidate resume upload & pipeline dispatch |
| `GET` | `/api/candidates` | Protected | Paginated candidate roster with score filters |
| `GET` | `/api/candidates/:id` | Protected | Complete candidate dossier & agent evaluation |
| `PATCH`| `/api/candidates/:id/stage`| Protected | Manual HR stage transition |
| `GET` | `/api/assessments/:token` | Public | Retrieve dynamically generated test |
| `POST` | `/api/assessments/:token/submit`| Public | Submit test answers (Triggers Agent 3) |
| `GET` | `/api/interviews/:token` | Public | Validate room token & load interview questions |
| `POST` | `/api/interviews/:token/complete`| Public | Save voice transcript (Triggers Agent 5) |
| `GET` | `/api/tenants/stats` | Protected | Aggregate analytics and hiring metrics |

---

## 🗺 End-to-End Workflow

```
1. Employer Setup           /register ────► Create organization and jobs
                                                │
2. Applicant Ingestion      /apply/:jobId ─► Candidate submits resume PDF
                                                │
3. Autonomous Swarm Run     Agent 1: In-memory AES-256 decryption & resume scoring
                                                ▼ (score >= 60)
                            Agent 2: Dynamic question synthesis & email dispatch
                                                ▼ (candidate submits test)
                            Agent 3: Answer grading & benchmark scoring
                                                ▼ (score >= 50)
                            Agent 4: Signed 24h voice interview room generation
                                                ▼ (voice interview completed)
                            Agent 5: Weighted holistic score & HIRE recommendation
                                                │
4. Executive Decision       /candidates/:id ─► HR reviews dossier and signs off (Offer / Reject)
```

---

<div align="center">

**AgenticHire** is engineered for high throughput, zero data loss, and bias-resistant recruitment.

Distributed Systems · Event-Driven Architecture · Autonomous AI Agents

</div>
