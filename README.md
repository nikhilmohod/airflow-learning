# airflow-learning
This repo contains Apache Airflow learning material and sample DAGs to help understand scheduling, orchestration, and automation. It's designed for both beginners and experienced users looking to explore real-world data pipeline scenarios and best practices.

---

# Step 1: Set Up Apache Airflow Locally
To begin your Airflow journey, follow this beginner-friendly guide to set up Apache Airflow on your local machine using Docker:

# 📖 Blog: How to Set Up Apache Airflow Locally for Learning and Testing
🛠️ What you'll achieve: [How to Set Up Apache Airflow Locally for Learning and Testing](https://medium.com/@nikhilmohod21/how-to-set-up-apache-airflow-locally-for-learning-and-testing-14fc5e0c8e4a)
  Install Airflow using the official Docker setup
  Understand folder structure and Airflow UI
  Prepare your environment for hands-on DAG execution

---

# Step 2: Clone & Use DAGs from Feature Branch
  Once Airflow is up and running locally:
  📁 Clone your Airflow-learning GitHub repository
      > git clone <your-airflow-learning-repo-url>
      > cd airflow-learning
  🌿 Switch to the feature branch
      > git checkout feature-airflow-dag
      
  📌 Copy your DAGs into the Airflow DAGs folder
      > cp dags/* ~/airflow/dags/
  🔄 Restart Airflow to load new DAGs:
      > docker-compose down
      > docker-compose up -d
      
🧪 Test the DAGs via the Airflow UI:
      > Go to http://localhost:8080 and enable your DAGs.

---

# ✅ Step 3: Practice and Iterate
Now that everything is set:
 # 💡 Start learning core Airflow concepts:
      DAGs (Directed Acyclic Graphs)
      Operators (Python, Bash, Databricks, Email)
      Task dependencies & scheduling
      Variables, Connections, XComs
      Sensors, Trigger Rules, Retry Logic
# 🔁 Practice Use Cases:
      Simulate ETL pipelines
      Trigger Databricks notebooks
      Add alerts/notifications
      Build task groups and branching logic

---

# 📁 Airflow 3 — Repo Structure

```
airflow-learning/
├── dags/
│   ├── ch01_linear_dag.py           # DAG basics & parsing
│   ├── ch02_dag_versioning.py       # DAG versioning (new in Airflow 3)
│   ├── ch03_operators_sync.py       # Operators & DAG syncing
│   ├── ch04_xcoms.py                # XComs with TaskFlow API
│   ├── ch05_manual_xcoms.py         # Manual XComs with kwargs
│   ├── ch06_parallel_tasks.py       # Parallel task execution
│   ├── ch07_conditional_branches.py # BranchPythonOperator
│   ├── ch08_scheduling_presets.py   # @daily, @hourly, @monthly etc.
│   ├── ch09_cron_syntax.py          # Cron expressions
│   ├── ch10_delta_trigger.py        # timedelta-based scheduling
│   ├── ch11_incremental_load_jinja.py # Jinja templates & incremental load
│   ├── ch12_special_schedules.py    # Event-based scheduling
│   └── ch13_assets.py               # Assets — data-aware scheduling
├── requirment.txt
└── README.md
```

---

# 🆕 Airflow 3 — What's New

| Feature | Description |
|---------|-------------|
| **Assets** | Replaces Datasets — trigger DAGs when data is updated, not just on a timer |
| **DAG Versioning** | Every deploy saves a new version — compare & track changes in the UI |
| **Task Execution API** | Workers receive tasks via REST API instead of direct DB access |
| **New React UI** | Faster interface with improved grid & graph views |
| **Edge Executor** | Run tasks on remote edge nodes outside the main cluster |
| **Python 3.9+** | Python 3.8 is no longer supported |

---

# 📚 Useful Links

| Resource | Link |
|----------|------|
| Official Docs | [airflow.apache.org/docs](https://airflow.apache.org/docs/) |
| Airflow 3 Migration Guide | [Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/migration-guide.html) |
| TaskFlow API Tutorial | [TaskFlow Docs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html) |
| Cron Expression Builder | [crontab.guru](https://crontab.guru) |
| Setup Blog | [Medium — Local Setup Guide](https://medium.com/@nikhilmohod21/how-to-set-up-apache-airflow-locally-for-learning-and-testing-14fc5e0c8e4a) |
| Docker Hub — Airflow | [hub.docker.com/r/apache/airflow](https://hub.docker.com/r/apache/airflow) |
