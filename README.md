# CO2 Emission ETL

This project is a Near Real-Time ETL pipeline that will do the necessary transformations of the raw data which is in the form of CSV files, the end goal here is to be able to calculate the total emission on a brand level, car level, and driver level. There are a lot of different tools and frameworks that are used to build ETL pipelines. In this project, the ETL is built using Python, Docker, PostgreSQL, and Airflow.

## Installation

1. Make sure you have [Docker](https://docs.docker.com/engine/install/) installed on your machine.
2. Change directories at the command line to be inside the `emission-etl` folder
```bash
cd emission-etl
```
3. In this step three services will get initialized, Jupyter Lab for testing, Postgres Database, and Pgadmin webserver to run queries on our tables.

      - We can access Jupyter Lab at [http://localhost:8080/](http://localhost:8080/)
      - Let's look at our database inside the pgadmin webserver 
        - Go to [http://localhost:5556/](http://localhost:5556/) and login with following credentials.
        ```
        email id: aniket@gmail.com
        password: password
        ```
        - Then follow the images for the next steps.
        ![step-1](pgadmin_steps/step_1.png)