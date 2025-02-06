# Senior Data Engineering Challenge

Hello! Welcome to Perpay's first senior and above data engineering challenge. We're hoping this will take you at most the span of a few hours, but this might depend on your level of familiarity with Docker. The challenge includes a section for Dockerizing a small application, writing some SQL code, and some follow-up questions. Feel free to use whatever resources you like and let us know if you have any questions.

### A Simple Scheduled SQL Job

We've created a template made up of various files and scripts for you to fill out to get to the intended final product. Once you share your changes with us, we should be able to bring up the whole system with:

```docker-compose -f docker-compose.yml up```

The goal here is to set up the following containers:

1. A PostgreSQL database.
1. An instance of Adminer we can access at `http://localhost:8080` and the necessary credentials to access the main database.
1. A python script to upload the data in `data/loans.csv` using `psycopg2` to a `loans` table.
1. A CRON job that triggers a python script every minute and writes job logs to a `cron.log` file within the container. (This python script will create a table named `default_liquidation_rolling_avg_hist`. More on this later.)

That means there should be 4 services in your `docker-compose.yml` file. You're welcome to re-arrange some of what we've created in this repo or you can stick to the framework available. Entirely up to you. Just make sure the entry point remains the `docker-compose.yml` file, and you use CRON for scheduling and SQL for analytics.

### The `default_liquidation_rolling_avg_hist` Table

The loans table includes the number of loans, liquidations, and defaults by day that we've observed between August 2021 and May 2023. We would like you to create a table that includes the liquidation to default ratio per day and the rolling average of this value using the last 7 days before the date of the row. We would also like you to include a timestamp for when the new table is created so that the changes that come through the CRON job are easy to differentiate. (We realize the data will not be changing, but this is a toy dataset after all.)

### Follow-up Questions

Please fill out your answers here in the README.md file under each question.

1. Let's say that the `loans` and `default_liquidation_rolling_avg_hist` tables were much larger and the latter required a far more complex and compute heavy query that used a similar logic for the dates. For example, maybe we updated the `loans` table every week with a week's worth of data. How would you go about optimizing such a query? What considerations would go into the type of database object you would use to store the data?

If possible we would move to a distributed system like AWS Redshift or Google Big Query which would allow us to leverage parallel processing across multiple nodes.  Secondly, depending on how complex the query gets, I would consider breaking the query up and saving the results into temp/permanent tables.  Having the database execute smaller chunks of logic at a time can sometimes boost performance especially if those chunks are limiting the record universe.  Finally, if applicable, we can create indexes on key columns.


2. If we wanted to convert this toy application to a real production application that pulled source tables such as the `loans` table from an external team's database into our own, how would you go about re-desisgning this system? What data/tech stack would you use and why? How would you improve data availability, freshness, and quality? How would you increase the observability of the data and data pipelines?

In terms of tech stack I would leverage Airflow first and foremost. Airflow would allow us to create a DAG per data set and provides a scheduler out of the box without messing with spinning up docker containers and cron jobs.  We could leverage Amazon's [MWWA](https://aws.amazon.com/managed-workflows-for-apache-airflow/) or Google's [Cloud Composer](https://cloud.google.com/composer?hl=en) which are fully managed Airflow instances.

Each DAG would extract from source, validate, execute transformations (if needed), load into the target database, and execute some final validations once in the target.  Validating the data at multiple points in the process is crucial for ensuring data availability and quality.  The idea is that we catch issues early and often and have a robust monitoring system to notify key team members before anyone realizes there is a problem downstream.

Using a version control system is also imperative and setting up CICD pipelines to deploy and test code changes streamlines updates and adds another layer of protection to our code.  Having a dev and a prod environment would also allow us to roll out code changes and fully test them in lower environments before propagating into the prod environment. This minimizes the risk of prod going off-line or prod data becoming compromised.

In terms of code, I would have some python code that multiple DAGs could share but ultimately separate each data set's ETL process within their own DAG.  Examples of some generalized helper py files would be
-  Database connection handler -  A single entry point which resolves multiple database connections in our stack
-  Generalized validation tools - Some validation tools out of the box that DAGs can leverage.  Things like `check_if_null` or `check_data_type`
- Monitoring tools - slack/email notifications when jobs/validations fail
