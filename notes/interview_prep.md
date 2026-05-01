# Interview Prep — Civic Ops Pipeline

## Core Concepts

### What is Docker?
Docker packages applications with their dependencies into containers
that run identically across any environment. It eliminates the
"works on my machine" problem and is the standard tool for modern
data infrastructure.

In my project: I run Postgres, Airflow, dbt, and Metabase as separate
containers, all coordinated via docker-compose. One command starts
the whole pipeline.

### What is an API?
An API (Application Programming Interface) is an interface that lets
programs request data from other systems over the internet, typically
returning JSON responses.

In my project: my Airflow DAG calls the NYC 311 API hourly to fetch
new service request records, then loads them into Postgres.