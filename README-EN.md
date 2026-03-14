<div align="center">

<img src="./static/image/MiroFish_logo_compressed.jpeg" alt="MiroFish Logo" width="75%"/>

<a href="https://trendshift.io/repositories/16144" target="_blank"><img src="https://trendshift.io/api/badge/repositories/16144" alt="666ghj%2FMiroFish | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

简洁通用的群体智能引擎，预测万物
</br>
<em>A Simple and Universal Swarm Intelligence Engine, Predicting Anything</em>

<a href="https://www.shanda.com/" target="_blank"><img src="./static/image/shanda_logo.png" alt="666ghj%2MiroFish | Shanda" height="40"/></a>

[![GitHub Stars](https://img.shields.io/github/stars/666ghj/MiroFish?style=flat-square&color=DAA520)](https://github.com/666ghj/MiroFish/stargazers)
[![GitHub Watchers](https://img.shields.io/github/watchers/666ghj/MiroFish?style=flat-square)](https://github.com/666ghj/MiroFish/watchers)
[![GitHub Forks](https://img.shields.io/github/forks/666ghj/MiroFish?style=flat-square)](https://github.com/666ghj/MiroFish/network)
[![Docker](https://img.shields.io/badge/Docker-Build-2496ED?style=flat-square&logo=docker&logoColor=white)](https://hub.docker.com/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/666ghj/MiroFish)

[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?style=flat-square&logo=discord&logoColor=white)](https://discord.com/channels/1469200078932545606/1469201282077163739)
[![X](https://img.shields.io/badge/X-Follow-000000?style=flat-square&logo=x&logoColor=white)](https://x.com/mirofish_ai)
[![Instagram](https://img.shields.io/badge/Instagram-Follow-E4405F?style=flat-square&logo=instagram&logoColor=white)](https://www.instagram.com/mirofish_ai/)

[English](./README-EN.md) | [中文文档](./README.md)

</div>

## ⚡ Overview

**MiroFish** is a next-generation AI prediction engine powered by multi-agent technology. By extracting seed information from the real world (such as breaking news, policy drafts, or financial signals), it automatically constructs a high-fidelity parallel digital world. Within this space, thousands of intelligent agents with independent personalities, long-term memory, and behavioral logic freely interact and undergo social evolution. You can inject variables dynamically from a "God's-eye view" to precisely deduce future trajectories — **rehearse the future in a digital sandbox, and win decisions after countless simulations**.

> You only need to: Upload seed materials (data analysis reports or interesting novel stories) and describe your prediction requirements in natural language</br>
> MiroFish will return: A detailed prediction report and a deeply interactive high-fidelity digital world

### Our Vision

MiroFish is dedicated to creating a swarm intelligence mirror that maps reality. By capturing the collective emergence triggered by individual interactions, we break through the limitations of traditional prediction:

- **At the Macro Level**: We are a rehearsal laboratory for decision-makers, allowing policies and public relations to be tested at zero risk
- **At the Micro Level**: We are a creative sandbox for individual users — whether deducing novel endings or exploring imaginative scenarios, everything can be fun, playful, and accessible

From serious predictions to playful simulations, we let every "what if" see its outcome, making it possible to predict anything.

## 🌐 Live Demo

Welcome to visit our online demo environment and experience a prediction simulation on trending public opinion events we've prepared for you: [mirofish-live-demo](https://666ghj.github.io/mirofish-demo/)

## 📸 Screenshots

<div align="center">
<table>
<tr>
<td><img src="./static/image/Screenshot/运行截图1.png" alt="Screenshot 1" width="100%"/></td>
<td><img src="./static/image/Screenshot/运行截图2.png" alt="Screenshot 2" width="100%"/></td>
</tr>
<tr>
<td><img src="./static/image/Screenshot/运行截图3.png" alt="Screenshot 3" width="100%"/></td>
<td><img src="./static/image/Screenshot/运行截图4.png" alt="Screenshot 4" width="100%"/></td>
</tr>
<tr>
<td><img src="./static/image/Screenshot/运行截图5.png" alt="Screenshot 5" width="100%"/></td>
<td><img src="./static/image/Screenshot/运行截图6.png" alt="Screenshot 6" width="100%"/></td>
</tr>
</table>
</div>

## 🎬 Demo Videos

### 1. Wuhan University Public Opinion Simulation + MiroFish Project Introduction

<div align="center">
<a href="https://www.bilibili.com/video/BV1VYBsBHEMY/" target="_blank"><img src="./static/image/武大模拟演示封面.png" alt="MiroFish Demo Video" width="75%"/></a>

Click the image to watch the complete demo video for prediction using BettaFish-generated "Wuhan University Public Opinion Report"
</div>

### 2. Dream of the Red Chamber Lost Ending Simulation

<div align="center">
<a href="https://www.bilibili.com/video/BV1cPk3BBExq" target="_blank"><img src="./static/image/红楼梦模拟推演封面.jpg" alt="MiroFish Demo Video" width="75%"/></a>

Click the image to watch MiroFish's deep prediction of the lost ending based on hundreds of thousands of words from the first 80 chapters of "Dream of the Red Chamber"
</div>

> **Financial Prediction**, **Political News Prediction** and more examples coming soon...

## 🔄 Workflow

1. **Graph Building**: Seed extraction & Individual/collective memory injection & GraphRAG construction
2. **Environment Setup**: Entity relationship extraction & Persona generation & Agent configuration injection
3. **Simulation**: Dual-platform parallel simulation & Auto-parse prediction requirements & Dynamic temporal memory updates
4. **Report Generation**: ReportAgent with rich toolset for deep interaction with post-simulation environment
5. **Deep Interaction**: Chat with any agent in the simulated world & Interact with ReportAgent

## 🏗️ System Architecture

### Layer Breakdown

| Layer | Core Modules | Responsibilities |
|------|--------------|------------------|
| Presentation | `frontend/src/views/*`, `frontend/src/components/*` | 5-step workflow UI, live simulation status, report and interaction pages |
| API | `backend/app/api/graph.py`, `simulation.py`, `report.py` | Public APIs for graph build, simulation control, report generation and download |
| Orchestration | `simulation_manager.py`, `simulation_runner.py` | Simulation state machine, process lifecycle, pause/resume/stop, live status aggregation |
| Memory & Graph | `graph_builder.py`, `zep_entity_reader.py`, `zep_graph_memory_updater.py` | Seed structuring, graph writing, entity filtering, and post-simulation memory writeback |
| Reasoning & Report | `report_agent.py`, `zep_tools.py`, `utils/llm_client.py` | ReACT multi-step reasoning, tool calls, and interactive prediction report generation |

### Project Code Structure Tree

```text
MiroFish/
├── frontend/                                  # Vue3 frontend project
│   ├── package.json                           # frontend dependencies and scripts
│   ├── vite.config.js                         # Vite build/dev server config
│   ├── index.html                             # frontend HTML entry
│   └── src/
│       ├── main.js                            # Vue app bootstrap
│       ├── App.vue                            # root component
│       ├── api/                               # backend API wrappers
│       │   ├── index.js                       # Axios instance and shared request config
│       │   ├── graph.js                       # graph build related APIs
│       │   ├── simulation.js                  # simulation control APIs
│       │   └── report.js                      # report generation/download/chat APIs
│       ├── router/
│       │   └── index.js                       # frontend routes
│       ├── store/
│       │   └── pendingUpload.js               # pending upload state store
│       ├── views/                             # page-level views
│       │   ├── Home.vue                       # home page
│       │   ├── MainView.vue                   # main workflow container
│       │   ├── Process.vue                    # 5-step process page
│       │   ├── SimulationView.vue             # simulation preparation page
│       │   ├── SimulationRunView.vue          # live simulation monitor page
│       │   ├── ReportView.vue                 # report viewer page
│       │   └── InteractionView.vue            # deep interaction page
│       ├── components/                        # business components
│       │   ├── Step1GraphBuild.vue            # Step1 graph build component
│       │   ├── Step2EnvSetup.vue              # Step2 environment setup component
│       │   ├── Step3Simulation.vue            # Step3 simulation component
│       │   ├── Step4Report.vue                # Step4 report component
│       │   ├── Step5Interaction.vue           # Step5 interaction component
│       │   ├── GraphPanel.vue                 # graph data panel
│       │   └── HistoryDatabase.vue            # historical memory/data panel
│       └── assets/logo/                       # frontend logo assets
│           ├── MiroFish_logo_left.jpeg
│           └── MiroFish_logo_compressed.jpeg
├── backend/                                   # Flask backend project
│   ├── run.py                                 # backend service entrypoint
│   ├── requirements.txt                       # Python dependency list
│   ├── pyproject.toml                         # Python project metadata/tooling
│   ├── uv.lock                                # uv-locked dependency versions
│   ├── app/
│   │   ├── __init__.py                        # Flask app factory and blueprint wiring
│   │   ├── config.py                          # backend config and env loading
│   │   ├── api/                               # API route layer
│   │   │   ├── __init__.py                    # Blueprint initialization
│   │   │   ├── graph.py                       # graph build and graph management endpoints
│   │   │   ├── simulation.py                  # entity read, simulation create/run/control endpoints
│   │   │   └── report.py                      # report generate/query/download/chat endpoints
│   │   ├── services/                          # core business services
│   │   │   ├── graph_builder.py               # GraphRAG graph build service
│   │   │   ├── ontology_generator.py          # ontology/entity type generation
│   │   │   ├── text_processor.py              # seed text cleaning/preprocessing
│   │   │   ├── zep_entity_reader.py           # Zep graph entity read/filter service
│   │   │   ├── oasis_profile_generator.py     # OASIS persona/profile generation
│   │   │   ├── simulation_config_generator.py # simulation config auto-generation
│   │   │   ├── simulation_manager.py          # simulation lifecycle state manager
│   │   │   ├── simulation_runner.py           # background simulation execution/monitoring
│   │   │   ├── simulation_ipc.py              # simulation process IPC protocol
│   │   │   ├── zep_graph_memory_updater.py    # write simulation actions back to graph memory
│   │   │   ├── zep_tools.py                   # ReportAgent tool integrations
│   │   │   └── report_agent.py                # ReACT report generation and Q&A service
│   │   ├── models/                            # state model layer
│   │   │   ├── __init__.py
│   │   │   ├── project.py                     # project state and metadata manager
│   │   │   └── task.py                        # async task state model
│   │   └── utils/                             # shared infrastructure utilities
│   │       ├── __init__.py
│   │       ├── llm_client.py                  # OpenAI-SDK-compatible LLM client
│   │       ├── file_parser.py                 # uploaded file parsing utilities
│   │       ├── logger.py                      # layered logging system
│   │       ├── retry.py                       # retry helpers/decorators
│   │       └── zep_paging.py                  # Zep paging helper
│   ├── scripts/                               # OASIS runtime scripts
│   │   ├── run_parallel_simulation.py         # Twitter + Reddit parallel simulation entry
│   │   ├── run_twitter_simulation.py          # Twitter simulation runner
│   │   ├── run_reddit_simulation.py           # Reddit simulation runner
│   │   ├── action_logger.py                   # agent action logging utility
│   │   └── test_profile_format.py             # profile format validation script
│   ├── uploads/                               # runtime data (projects/simulations/reports)
│   └── logs/                                  # backend runtime logs
├── static/
│   └── image/                                 # README images and demo assets
├── package.json                               # root-level scripts for frontend/backend
├── docker-compose.yml                         # Docker orchestration (frontend + backend)
├── Dockerfile                                 # Docker image build definition
├── .env.example                               # environment variable template
├── README.md                                  # Chinese documentation
├── README-EN.md                               # English documentation
└── LICENSE                                    # open-source license
```

## 🚀 Quick Start

### Option 1: Source Code Deployment (Recommended)

#### Prerequisites

| Tool | Version | Description | Check Installation |
|------|---------|-------------|-------------------|
| **Node.js** | 18+ | Frontend runtime, includes npm | `node -v` |
| **Python** | ≥3.11, ≤3.12 | Backend runtime | `python --version` |
| **uv** | Latest | Python package manager | `uv --version` |

#### 1. Configure Environment Variables

```bash
# Copy the example configuration file
cp .env.example .env

# Edit the .env file and fill in the required API keys
```

**Required Environment Variables:**

```env
# LLM API Configuration (supports any LLM API with OpenAI SDK format)
# Recommended: Alibaba Qwen-plus model via Bailian Platform: https://bailian.console.aliyun.com/
# High consumption, try simulations with fewer than 40 rounds first
LLM_API_KEY=your_api_key
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
LLM_MODEL_NAME=qwen-plus

# Zep Cloud Configuration
# Free monthly quota is sufficient for simple usage: https://app.getzep.com/
ZEP_API_KEY=your_zep_api_key
# Graph backend choice (pick one, defaults to Zep)
# To use RAGflow instead, set GRAPH_BACKEND=ragflow; ZEP_API_KEY is then not required
# GRAPH_BACKEND=ragflow
# RAGFLOW_BASE_URL=http://localhost
# RAGFLOW_API_KEY=your-ragflow-api-key
```

#### 2. Install Dependencies

```bash
# One-click installation of all dependencies (root + frontend + backend)
npm run setup:all
```

Or install step by step:

```bash
# Install Node dependencies (root + frontend)
npm run setup

# Install Python dependencies (backend, auto-creates virtual environment)
npm run setup:backend
```

#### 3. Start Services

```bash
# Start both frontend and backend (run from project root)
npm run dev
```

**Service URLs:**
- Frontend: `http://localhost:3000`
- Backend API: `http://localhost:5001`

**Start Individually:**

```bash
npm run backend   # Start backend only
npm run frontend  # Start frontend only
```

### Option 2: Docker Deployment

```bash
# 1. Configure environment variables (same as source deployment)
cp .env.example .env

# 2. Pull image and start
docker compose up -d
```

Reads `.env` from root directory by default, maps ports `3000 (frontend) / 5001 (backend)`

> Mirror address for faster pulling is provided as comments in `docker-compose.yml`, replace if needed.

## 📬 Join the Conversation

<div align="center">
<img src="./static/image/QQ群.png" alt="QQ Group" width="60%"/>
</div>

&nbsp;

The MiroFish team is recruiting full-time/internship positions. If you're interested in multi-agent simulation and LLM applications, feel free to send your resume to: **mirofish@shanda.com**

## 📄 Acknowledgments

**MiroFish has received strategic support and incubation from Shanda Group!**

MiroFish's simulation engine is powered by **[OASIS (Open Agent Social Interaction Simulations)](https://github.com/camel-ai/oasis)**, We sincerely thank the CAMEL-AI team for their open-source contributions!

## 📈 Project Statistics

<a href="https://www.star-history.com/#666ghj/MiroFish&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=666ghj/MiroFish&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=666ghj/MiroFish&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=666ghj/MiroFish&type=date&legend=top-left" />
 </picture>
</a>
