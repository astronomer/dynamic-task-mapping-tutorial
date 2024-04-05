Overview
========

Welcome to Astronomer! This project was generated after running 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

The purpose of this project is to demonstrate the use of the dynamic task mapping feature. For more on this feature, see [this guide](https://docs.astronomer.io/learn/dynamic-tasks).

Project Contents
================

Your Astronomer project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. This project contains two DAGs that demonstrate how to implement dynamic task mapping:
  - `mapping-example.py`: contains a simple Python function showing how to use the `expand()` mapping function.
  - `mapping-elt.py`: shows a real-world ELT example use case of loading files from S3 to Snowflake and then processing the data within Snowflake. The number of files in S3 is unknown and variable, so we use dynamic task mapping to dynamically create the tasks at run time.
- Dockerfile: This file contains a versioned Astronomer Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. In this project it contains the `transformation_query.sql` script called by the `mapping-elt.py` DAG.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. For this project, the `apache-airflow-providers-snowflake` is needed.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/ . A [free trial of Astro](https://astronomer.io/try-astro) is available.

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
