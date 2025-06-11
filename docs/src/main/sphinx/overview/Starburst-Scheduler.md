
---

```markdown
# Starburst Scheduler: Python Pip Package to Automate Starburst.io SQL Queries


## Meta Description

Starburst Scheduler is an open-source Python package that enables you to run and schedule SQL queries on Starburst Trino OSS and Starburst Enterprise. Automate Starburst queries, monitor cluster health, and integrate with Slack and Mattermost — all from a simple CLI tool. Ideal for DevOps, data engineering, and CI/CD pipelines.


## Introduction

In today’s data-driven organizations, query automation and monitoring are critical for maintaining healthy data platforms and delivering reliable insights.

**Common questions:**

- How do I automate important queries to monitor our Starburst clusters?
- How do I run scheduled reports and send alerts to Slack or Mattermost?

While tools like Apache Airflow exist, I wanted something much lighter and faster — something that could run from the command line or within CI/CD pipelines.

That’s why I built **Starburst Scheduler** — an open-source Python package that enables anyone to run and schedule Starburst queries with just a few commands.

In this article, I’ll walk you through:

- What is Starburst
- Why I built this tool
- How Starburst Scheduler works
- How you can use it
- Planned enhancements
- How you can contribute

## What is Starburst?

Starburst is a modern distributed query engine built on top of Trino.

It allows users to run high-performance SQL queries across multiple data sources, including:

- Data lakes (S3, ADLS)
- Hive
- Delta Lake
- PostgreSQL
- MySQL
- Oracle
- Kafka
- And more

It powers interactive analytics and data mesh architectures in modern enterprises.

### Starburst Galaxy vs Starburst Enterprise vs OSS

- **Starburst Galaxy** → fully managed SaaS offering — ideal for cloud-native deployments.
- **Starburst Enterprise** → self-managed version — deployable on Kubernetes, OpenShift, or on-premises.
- **OSS (Open Source Trino)** → provides the core engine but lacks the enterprise features, support, and integrations.

## The Need for Automation

In production data platforms, teams often need to:

- Monitor cluster health via scheduled queries
- Run daily reports and publish results
- Detect data anomalies and trigger alerts
- Integrate query result with team communication tools (Slack, Mattermost)

While Starburst provides an excellent UI, it does not natively offer lightweight scheduling.

Apache Airflow is an option, but requires significant setup.  
Cron jobs are too basic and hard to manage at scale.

That’s why I built this simple CLI tool.

## What is Starburst Scheduler?

Starburst Scheduler is an open-source Python package that allows you to:

- Run Starburst queries from the command line
- Schedule Starburst queries to run at regular intervals (seconds, minutes, hours, days)
- (Coming soon) Send results to Slack, Mattermost, email

It is ideal for:

- DevOps engineers monitoring Starburst clusters
- Data engineers automating reports
- Platform engineers adding checks to CI/CD pipelines

It requires no additional servers or orchestration systems.

## How It Works

Starburst Scheduler is built using:

- `pystarburst` — the official Python client for Starburst
- `schedule` — a lightweight job scheduling library
- `click` — for building powerful command-line interfaces

### Architecture

```

+----------------------+     +------------------+
\| CLI (Click)          | --> | StarburstConnector|
\|                      |     +------------------+
\| run-query            | --> Connect --> Run query
\| schedule-query       | --> Connect --> Schedule query
+----------------------+                |
v
+--------------------+
\| QueryScheduler     |
\| (schedule library) |
+--------------------+

````

## Features

- Run SQL queries directly from the CLI
- Schedule queries at regular intervals (seconds, minutes, hours, days)
- Compatible with both Starburst Galaxy and Starburst Enterprise
- Works well in CI/CD pipelines (GitHub Actions, Jenkins, etc.)
- Lightweight — no servers required
- (Coming soon) Slack/Mattermost integration — automated alerts
- MIT Licensed — fully open-source

## Installation

```bash
pip install starburst_scheduler
````

## Usage

### Run a Single Query

```bash
starburst-scheduler run-query --host <host> --port <port> --user <user> --password <password> --catalog sample --schema burstbank --query "SELECT * FROM system.runtime.nodes"
```

### Schedule a Query

```bash
starburst-scheduler schedule-query --host <host> --port <port> --user <user> --password <password> --catalog sample --schema burstbank --query "SELECT * FROM system.runtime.nodes" --frequency 60 --time-unit seconds
```

## Planned Enhancements

### Slack & Mattermost integration

Automatically post query results to team chat when queries run.
Example: monitor active queries, cluster status, send alerts.

### CSV/JSON output

Option to save query results to CSV or JSON for downstream use in data pipelines.

### Advanced scheduling

Support for cron expressions, weekly/monthly jobs.

### Email notifications

Option to send query results via email.

### Error alerting

Notify users when query runs fail.

## Example Use Cases

### Cluster Health Monitoring

Run this query every 5 minutes:

```sql
SELECT * FROM system.runtime.nodes
```

Send the results to Slack to monitor Starburst node status.

### Daily Report Automation

Schedule a complex query to run daily at 6 AM, save the results as CSV, and email them to the BI team.

## How to Contribute

GitHub: [https://github.com/karranikhilreddy99/starburst\_scheduler](https://github.com/karranikhilreddy99/starburst_scheduler)

* Issues and PRs welcome
* If you like the project, please star it on GitHub

## Conclusion

Starburst Scheduler fills an important gap:

* It allows you to automate Starburst queries without setting up heavy orchestration tools
* It is ideal for DevOps, data engineering, and CI/CD pipelines
* It is fully open-source and easy to extend

In the future, I plan to add Slack/Mattermost integrations, advanced scheduling, and more.

If you use Starburst and want a lightweight scheduling tool — give **Starburst Scheduler** a try.

GitHub: [https://github.com/karranikhilreddy99/starburst\_scheduler](https://github.com/karranikhilreddy99/starburst_scheduler)
