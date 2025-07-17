# airflow-learning
This repo contains Apache Airflow learning material and sample DAGs to help understand scheduling, orchestration, and automation. It’s designed for both beginners and experienced users looking to explore real-world data pipeline scenarios and best practices.

# Step 1: Set Up Apache Airflow Locally
To begin your Airflow journey, follow this beginner-friendly guide to set up Apache Airflow on your local machine using Docker:

# 📖 Blog: How to Set Up Apache Airflow Locally for Learning and Testing
🛠️ What you’ll achieve: [How to Set Up Apache Airflow Locally for Learning and Testing](https://medium.com/@nikhilmohod21/how-to-set-up-apache-airflow-locally-for-learning-and-testing-14fc5e0c8e4a)
  Install Airflow using the official Docker setup
  Understand folder structure and Airflow UI
  Prepare your environment for hands-on DAG execution

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

