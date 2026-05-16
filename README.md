# What's On Eire

A real-time, serverless event aggregation and analytics platform designed to surface hyper-local events across every city and county in Ireland. 

The platform bridges the gap between major urban festivals and grassroots community gatherings by pulling fragmented public data into a unified, high-throughput streaming ecosystem.

## 🏗️ Architecture & Tech Stack

This project is built using a modern, big-data serverless architecture:

*   **Language & Runtime:** Scala 3 (Targeting Java 17/21 JVM) & SBT
*   **Ingestion & Streaming:** AWS Lambda, AWS EventBridge, Amazon Kinesis Data Streams
*   **Storage & Read Layer:** Amazon DynamoDB (Single-table design optimized for regional queries)
*   **Big Data & Analytics:** Databricks (Apache Spark) running regional trend analysis via an S3 data lake
*   **Infrastructure:** LocalStack (for offline emulation) & Terraform / AWS CDK

---

## 🗺️ Targeted Data Sources

To create a truly localized map of Ireland, the app scales out by connecting to diverse commercial and open data feeds:

1.  **Ticketmaster API:** High-profile commercial entertainment, gigs, and sports.
2.  **Meetup API:** Grassroots local groups, tech meetups, and social gatherings.
3.  **Fáilte Ireland Open Data:** Official cultural festivals and regional tourism events.
4.  **Data.gov.ie:** Local municipal council feeds and county border datasets.
5.  **OpenStreetMap API:** Reverse-geocoding coordinates directly into specific Irish county bounds.

---

## 🛠️ Current Project Status: Stage 1 (Step 1 Complete)

The core environment setup and API data exploration phases are currently complete. We have isolated the raw API data shapes and established an isolated, secure workspace workspace inside `http/`.

### Directory Layout
```text
rootDir/
├── .vscode/
│   └── settings.json       # Workspace extensions setup
├── http/
│   ├── .env.template       # Public blueprint for credentials
│   ├── .env               # Local secret variables (GIT IGNORED)
│   └── payload-ticketmaster.http  # Executable REST client file
└── .gitignore              # Strict environment & cache filters
```

🚀 Getting Started (Local Exploration)
To test the live API endpoints locally without writing code yet, we utilize the VS Code REST Client extension.

Prerequisites
Install VS Code.

Install the REST Client extension by Huachao Mao via the marketplace.

Obtain a free developer key from the Ticketmaster Developer Portal.

Setup Instructions
Clone the repository:

Bash
git clone <your-repo-url>
cd discover-ireland-localized


2. **Configure your Secrets:**
   Inside the `http/` folder, create a file named `.env` mirroring the structure of `.env.template`:
   ```text
   TICKETMASTER_API_KEY=your_actual_api_key_here
   
Execute the Query:

Open http/payload-ticketmaster.http.

Click the Send Request link visible right above the GET method.

A split view window will instantly display the raw payload map of active events across Ireland.

🔒 Security Guardrails
Zero Leak Policy: The .gitignore file is explicitly mapped to prevent tracking .env files or temporary JSON data dumps (*.json).

Always execute changes using .env.template references when updating structural API query adjustments to the code baseline.

📈 Next Up: Stage 1, Step 2
Implementing the Unified Data Schema using Scala 3 Case Classes and structural enums representing the 32 counties.

Integrating JSON parsing engines (Circe) to map the dynamic incoming JSON strings into a unified record format.