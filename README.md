# 🤖 Agentic CI/CD Orchestrator

An autonomous agentic platform using **Java (Spring Boot)** and **LangGraph4j** to intercept high-frequency code commits, orchestrate multi-stage CI/CD build workflows, and perform real-time dependency validation.

## Architecture

```
+------------------------------------------------------------------------+
|                          Event Sources                                 |
|  +--------------+  +------------------+  +----------------------------+|
|  | GitHub       |  | GitLab           |  | Manual Trigger             ||
|  | Webhooks     |  | Webhooks         |  | REST API                   ||
|  +------+-------+  +--------+---------+  +-------------+--------------+|
+---------|--------------------|--------------------------|---------------+
          |                    |                          |
          v                    v                          v
+------------------------------------------------------------------------+
|                     Apache Kafka Event Bus                             |
|   +-----------------+ +------------------+ +-------------------------+ |
|   | commit-events   | | pipeline-events  | | feedback-loop           | |
|   +--------+--------+ +--------+---------+ +------------+------------+ |
+------------|-------------------|-----------------------|----------------+
             |                   |                       |
             v                   v                       v
+------------------------------------------------------------------------+
|              Agentic Pipeline Controller (Spring Boot)                 |
|                                                                        |
|  +------------------------------------------------------------------+ |
|  |                   LangGraph4j State Graph                        | |
|  |                                                                  | |
|  |  +----------+   +----------+   +----------+   +--------------+  | |
|  |  | Commit   |-->|Dependency|-->|  Build   |-->| Test Runner  |  | |
|  |  | Analyzer |   |Validator |   |Orchestr. |   |   Agent      |  | |
|  |  +----------+   +----------+   +----------+   +-------+------+  | |
|  |                                                        |         | |
|  |                    +----------+   +--------------+     |         | |
|  |                    |Deployment|<--|  PR Comment  |<----+         | |
|  |                    |  Gate    |   |  Summarizer  |               | |
|  |                    +----------+   +--------------+               | |
|  +------------------------------------------------------------------+ |
|                                                                        |
|  +----------------------------------+  +-----------------------------+ |
|  |  Stateful Workflow Manager       |  |  Observability Layer        | |
|  |  - Multi-env state tracking      |  |  - Prometheus metrics       | |
|  |  - Workflow persistence          |  |  - Grafana dashboards       | |
|  |  - Retry / rollback logic        |  |  - Agent performance KPIs   | |
|  +----------------------------------+  +-----------------------------+ |
+------------------------------------------------------------------------+
```

## LangGraph4j Agent Nodes

| Node | Purpose | Transitions |
|------|---------|-------------|
| **CommitAnalyzer** | Parses commit metadata, detects change scope | → DependencyValidator |
| **DependencyValidator** | Validates dependency tree, detects conflicts | → BuildOrchestrator (pass) / → CommitAnalyzer (fail + fix) |
| **BuildOrchestrator** | Orchestrates multi-stage builds across environments | → TestRunner |
| **TestRunner** | Triggers recursive test suites, collects results | → PRCommentSummarizer (pass) / → BuildOrchestrator (retry) |
| **PRCommentSummarizer** | Generates AI-powered PR review summaries | → DeploymentGate |
| **DeploymentGate** | Evaluates deployment readiness, approves/blocks | → END (approved) / → HITL (needs approval) |

## Tech Stack

- **Core**: Java 21 + Spring Boot 3.3
- **Agent Framework**: LangGraph4j + LangChain4j
- **Event Bus**: Apache Kafka (Redpanda)
- **Observability**: Prometheus + Grafana + Micrometer
- **LLM**: OpenAI / Anthropic / Google Gemini (configurable)

## Quick Start

```bash
# 1. Start infrastructure (Kafka + Prometheus + Grafana)
docker-compose up -d

# 2. Build and run
./gradlew bootRun

# 3. Trigger a pipeline manually
curl -X POST http://localhost:8080/api/v1/pipelines/trigger \
  -H "Content-Type: application/json" \
  -d '{"repo": "org/my-repo", "branch": "main", "commitSha": "abc123", "environment": "production"}'

# 4. Approve or reject a Human-in-the-Loop production gate
curl -X POST http://localhost:8080/api/v1/pipelines/{pipelineId}/approve \
  -H "Content-Type: application/json" \
  -d '{"approver": "lead-engineer", "notes": "Security review verified"}'

# 5. View Grafana dashboards
open http://localhost:3001  # admin/admin
```

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/pipelines/trigger` | Trigger a new pipeline execution with optional environment and approval flags |
| `GET` | `/api/v1/pipelines` | List all active and historic pipeline states |
| `GET` | `/api/v1/pipelines/{id}` | Get status and execution state of a specific pipeline |
| `GET` | `/api/v1/pipelines/{id}/report` | Retrieve aggregated security audit and dependency evaluation report |
| `GET` | `/api/v1/pipelines/{id}/pr-comment` | Fetch generated Markdown PR review comment |
| `POST` | `/api/v1/pipelines/{id}/approve` | **Human-in-the-Loop**: Approve a held deployment gate |
| `POST` | `/api/v1/pipelines/{id}/reject` | **Human-in-the-Loop**: Reject a held deployment gate |
| `DELETE` | `/api/v1/pipelines/{id}` | Cancel an active pipeline execution |
| `POST` | `/api/v1/webhooks/github` | Receive GitHub `push` and `pull_request` webhooks with HMAC-SHA256 signature verification |

## Observability

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3001 (admin/admin)
- **Kafka Console**: http://localhost:8888

### Key Metrics
- `cicd_pipeline_duration_seconds` — End-to-end pipeline execution time
- `cicd_agent_node_duration_seconds` — Per-node execution time
- `cicd_agent_code_analysis_duration_seconds` — LLM code analysis duration
- `cicd_agent_reviews_total` — Total AI reviews by risk assessment
- `cicd_dependency_vulnerabilities_total` — Real-time vulnerability detection counts
- `cicd_builds_total` — Total builds by status (success/fail)
- `cicd_tests_total` — Test execution counts

## High-Performance Execution & Safety

- **Java 21 Virtual Threads**: Non-blocking concurrent execution of independent DAG tasks via `Executors.newVirtualThreadPerTaskExecutor()`.
- **Topological Cycle Detection**: Kahn's algorithm dynamically validates pipeline task topologies before dispatch, preventing runtime deadlocks.
- **LLM Code Analysis Agent**: Asynchronous heuristic and LLM-powered security auditing to detect hardcoded credentials, SQL injection, and command injection patterns directly from git diffs.
- **Automated Dependency Validation**: Real-time gating against SNAPSHOT versions and critical vulnerabilities (e.g. Log4Shell, Spring4Shell, Text4Shell, Jackson deserialization).
- **Automated PR Review Dispatcher**: Asynchronously posts rich collapsible Markdown code reviews directly to GitHub Pull Requests via WebClient with dry-run fallback.
- **Human-in-the-Loop (HITL) Deployment Gate**: Policy-driven gates enforcing automatic blocks on critical vulnerabilities, auto-approvals on staging, and cryptographic approval holds for production deployments.

## License

MIT
