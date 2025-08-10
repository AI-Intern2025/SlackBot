# SlackBot – AI-powered Slack Assistant for Data Analysis and Monitoring

A Slack app that analyzes natural-language requests, proposes safe SQL, executes with approval, visualizes results, and sends automated anomaly alerts to your channels.

## 🎯 Features

### Core Capabilities

- **App Mentions → Insights**: Ask questions by mentioning the bot; get SQL + explanations before execution
- **Human-in-the-loop Safety**: Approve/deny generated queries prior to running on the database
- **Visual Responses**: Returns charts/plots for supported analyses
- **Slash Commands**:
  - `/check-anomalies` — run automated anomaly checks
  - `/check-metric [metric]` — analyze a specific business metric
  - `/create-alert [...]` — create monitoring alerts (can open interactive flows)
- **Event Handling**: Listens to app_mention and interactive events
- **Environment-driven Config**: Easily configure models, DB URL, sampling, verbosity

### Advanced Features

- **Automated Monitoring**: Scheduled checks and daily digests via a monitoring engine
- **Alerts to Channels**: Real-time Slack alerts to configured channels (e.g., #alerts)
- **SQL Assist + Guardrails**: SQL generation with explanations, size limits, sampling, and safe defaults
- **Embeddings & Similarity**: Optional FAISS-backed schema/context similarity for better query guidance
- **Extensible Agents**: LangChain-powered flows to plug in new tools and data sources

## 📱 Usage Highlights

- **Mention the bot for analysis**: "@YourApp analyze daily deposits as a chart"
- **Run anomaly checks**: `/check-anomalies`
- **Deep-dive a metric**: `/check-metric daily_active_users`
- **Create alerts quickly**: `/create-alert Track payment errors above 50 critical realtime`

## 🏗️ Architecture

### Tech Stack

- **Slack**: Slack Bolt (Flask adapter), Events, Interactivity, Slash Commands
- **AI/Agents**: LangChain + OpenAI
- **Data**: SQLAlchemy + Postgres (psycopg2), sqlglot for SQL manipulation
- **Monitoring**: schedule/croniter for jobs and digests
- **Visualization**: matplotlib for charts
- **Optional**: FAISS for embeddings-based similarity/context

### Key Components

- `app.py`: Entry point, Slack events/interactions, routes (/slack/events, /slack/interactions)
- `agent.py`: Agent reasoning, SQL proposal, human approval flow
- `anomaly_detector.py`: Metrics anomaly detection logic
- `automated_monitor.py`: Schedulers, daily digests, alert jobs
- `viz.py`: Chart building and image responses
- `utils.py`: Shared helpers (DB, embeddings/FAISS, SQL utilities)

## 📚 Documentation

All setup and configuration instructions (local run, environment variables, Slack app configuration, ngrok, and troubleshooting) live in the documentation folder:

- `documentation/Project-Setup-Deployment-Guide.pdf`
- `documentation/SLACK-APP-GUIDE.pdf`

Please refer to those documents for end-to-end setup and Slack console screenshots.

## 🔐 Security

- Keep secrets (`SLACK_BOT_TOKEN`, `SLACK_SIGNING_SECRET`, `OPENAI_API_KEY`, `DATABASE_URL`) out of version control
- Use least-privilege DB users in production
- Rotate credentials if exposure is suspected

## 🗂️ Repository Structure

```
├── src/
│   ├── app.py
│   ├── agent.py
│   ├── anomaly_detector.py
│   ├── automated_monitor.py
│   ├── viz.py
│   └── utils.py
├── requirements.txt
├── documentation/
│   ├── Project-Setup-Deployment-Guide.pdf
│   └── SLACK-APP-GUIDE.pdf
├── .gitignore
└── envStructure.txt
```