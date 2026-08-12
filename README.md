# 🤖 Agentic CI/CD Orchestrator

An autonomous agentic platform using **Java (Spring Boot)** and **LangGraph4j** to intercept high-frequency code commits, orchestrate multi-stage CI/CD build workflows, and perform real-time dependency validation.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Event Sources                                     │
│  ┌──────────────┐  ┌──────────────────┐  ┌────────────────────────────┐  │
│  │ GitHub        │  │ GitLab           │  │ Manual Trigger             │  │
│  │ Webhooks      │  │ Webhooks         │  │ REST API                   │  │
│  └──────┬───────┘  └────────┬─────────┘  └─────────────┬──────────────┘  │
└─────────┼──────────────────┼───────────────────────────┼─────────────────┘
          │                  │                           │
          ▼                  ▼                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Apache Kafka Event Bus                                 │
│   ┌─────────────────┐ ┌──────────────────┐ ┌─────────────────────────┐  │
│   │ commit-events   │ │ pipeline-events  │ │ feedback-loop           │  │
│   └────────┬────────┘ └────────┬─────────┘ └────────────┬────────────┘  │
└────────────┼───────────────────┼────────────────────────┼────────────────┘
             │                   │                        │
             ▼                   ▼                        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              Agentic Pipeline Controller (Spring Boot)                    │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                   LangGraph4j State Graph                          │  │
│  │                                                                    │  │
│  │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐  │  │
│  │  │ Commit   │──▶│Dependency│──▶│  Build   │──▶│ Test Runner  │  │  │
│  │  │ Analyzer │   │Validator │   │Orchestr. │   │   Agent      │  │  │
│  │  └──────────┘   └──────────┘   └──────────┘   └───────┬──────┘  │  │
│  │                                                       │          │  │
│  │                    ┌──────────┐   ┌──────────────┐    │          │  │
│  │                    │Deployment│◀──│  PR Comment  │◀───┘          │  │
│  │                    │  Gate    │   │  Summarizer  │               │  │
│  │                    └──────────┘   └──────────────┘               │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌──────────────────────────────────┐  ┌─────────────────────────────┐  │
│  │  Stateful Workflow Manager       │  │  Observability Layer        │  │
│  │  • Multi-env state tracking      │  │  • Prometheus metrics       │  │
│  │  • Workflow persistence          │  │  • Grafana dashboards       │  │
│  │  • Retry / rollback logic        │  │  • Agent performance KPIs   │  │
│  └──────────────────────────────────┘  └─────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
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
  -d '{"repo": "org/my-repo", "branch": "feature/new-api", "commitSha": "abc123"}'

# 4. View Grafana dashboards
open http://localhost:3001  # admin/admin
```

## Observability

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3001 (admin/admin)
- **Kafka Console**: http://localhost:8888

### Key Metrics
- `cicd_pipeline_duration_seconds` — End-to-end pipeline execution time
- `cicd_agent_node_duration_seconds` — Per-node execution time
- `cicd_builds_total` — Total builds by status (success/fail)
- `cicd_tests_total` — Test execution counts
- `cicd_pr_comments_total` — Auto-generated PR summaries
- `cicd_deployment_gates_total` — Deployment decisions

## License

MIT
