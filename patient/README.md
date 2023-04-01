# Solution to Assignment

## Sections
1. [TL;DR version](#tldr)  
2. [Details](#details)  
3. [Architectural choices](#architectural)  
4. [Replication steps](#replication)

## TL;DR version

![High Level architecture](/images/arch_hl.jpeg)

The design involves building a data pipeline that pulls data from API using async calls and streams that data to a Kafka topic. The Kafka topic then puts the data into a PostgreSQL database using a JDBC sink.

## Details

The data pipeline follows these main steps:  

**Async Calls to API**: The data pipeline makes async calls to an API endpoint to fetch data. This is done using the aiohttp library which provides a simple interface to perform asynchronous HTTP requests. I used asyncio to handle the async calls, which allows us to make multiple requests in parallel.
Since the actual data is not in the top level of the API, I took the following steps:  
1. Do a sequential pull to retrieve nextpageTokens for the 10 pages.  
2. Do Async calls to all the 10 pages to retrieve the medialinks for 10000 documents.  
3. Do async calls to 10000 document urls to retrieve the data.  

The documents are parsed using regex to retrieve values for id, name, age, institution, activity and comment fields. The values then go through cursory clean up and the *name field is redacted for PII sanitization*. Then the data point (dict) is handed over to producer application.  

**Publishing to Kafka**: Once the data is fetched from the API, it is serialized to Avro format and published to a Kafka topic using the confluent-kafka library. The Kafka topic acts as a buffer for the data and ensures that the data is delivered reliably and efficiently to downstream systems. Avro serialisation helps with lesser network bandwidth consumption and support for schema evolution. The kafka setup I have used is modelled after confluent kafka's docker setup.   

**Inserting into PostgreSQL**: The data is then inserted into a PostgreSQL database using a JDBC sink connector for kafka. The JDBC connector allows us to easily connect to the PostgreSQL database and insert data efficiently and reliably. It allows for a lot of customizations such as auto create, auto evolve, insert mode, deserialisation etc.  

**Error Handling and Logging**: The data pipeline includes error handling and logging to ensure that any errors or issues are logged and can be addressed. If an error occurs during the pipeline execution, it is logged with detailed information to help with debugging and troubleshooting.

## Architectural

### Async Calls
To pull data from the API efficiently, I use async calls instead of synchronous calls. This allows us to make multiple requests at the same time without waiting for each request to complete before sending the next one. This improves the performance of the data pipeline and allows us to handle large amounts of data more efficiently. The process takes around 1 minute to complete end to end as opposed to 20 minutes for the sequential pull.  

### Kafka
I use Kafka as the messaging system for the data pipeline. Kafka helps with decoupling the API data pull and database feed. Therefore, we can handle large volumes of data and ensure that data is delivered reliably and efficiently. We can also easily scale the data pipeline by adding more Kafka brokers to handle more data and use paritioned topics for a better performance. It provides robust and fast connectors to sink data to a wide variety of destinations.  

### PostgreSQL
I wanted to choose a SQL database here since we dont exactly have a known data access pattern and more likely will be used for analysis. Postgres sounded a fair choice because it would allow for faster dmls being transactional in nature and shows fair performance in aggregations as well.  

## Replication

1. Run `docker-compose up -d`.
This step might take a few minutes based on the network speed since the confluent images are bulky.  
2. When done, run: `docker-compose ps` to check the state of the containers. Ideally it should look like this:  
![docker-compose-ps](/images/docker-compose-ps-op.png)  
If any containers exit, feel free to fire `docker-compose up -d` again.  
3. On a browser, enter: `http://localhost:9021`. Since, it could take a few seconds for the broker to be registered completely, you might see something like this:  

![9021-unhealthy](/images/9021-unhealthy.png)  

Wait for a few seconds, it will turn to something like this:  

![9021-healthy](/images/9021-healthy.png)  

4. Now that the cluster is stable, we will create a connector. Before that, we'll get the ip of the postgres container.  
On a local bash terminal, run: `docker inspect postgresql -f "{{json .NetworkSettings.Networks }}" | jq '.async_stream_pg_default."Gateway"'`. You would get an IP in output. Note it down.  
Create the connector with the following curl to kafka connect from the local bash terminal:  
`curl -X PUT http://localhost:8083/connectors/patient-sink-postgres/config   
    -H "Content-Type: application/json"   
    -d '{  
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",  
        "connection.url": "jdbc:postgresql://**<enter_ip_here>**:2000/",  
        "connection.user": "postgres",  
        "connection.password": "password",  
        "tasks.max": "1",  
        "topics": "patient",  
        "auto.create": "true",  
        "auto.evolve":"true",  
        "insert.mode": "insert",  
        "table.name.format":"patient"  
    }'`  
**Note**: Replace  <enter_ip_here> with the IP noted before.  
As output, you would see a json with the summary of the connector.  

5. Return to the browser to the control-center console. To verify that the connector is successfully created and running, select the cluster => select Connect on left panel => select connect-default, you should see the connector created in last step in running state, e.g.  

![connector-state](/images/connector-state.png)  

6. Now, the pipeline is up. We just need to execute our script to flow the data through it.  
But first, on your bash terminal, run: `docker exec -it postgresql /bin/bash` and then `psql -h localhost -U postgres`, this will get you to the psql terminal. Run `\dt` to verify if any tables exist already. Since, its a fresh instance, you won't see any tables.  

![no-tables](/images/no-tables.png)  

On another bash terminal, run: `docker exec -it async_stream_pg_container python /home/app/async_pull.py`. Do not close the terminal. You would see logs from the execution like below:  

![script-logs](/images/script-run-op.png)  

Now, finally verify the table in postgres.  

![table-created](/images/table-created.png)  

![table-count](/images/table-count.png) 


