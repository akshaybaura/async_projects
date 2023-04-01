### Use this to fetch the gateway opened to access postgres in the network
docker inspect postgresql -f "{{json .NetworkSettings.Networks }}" | jq '.async_stream_pg_default."Gateway"'

### Replace the IP in connection url with value otained above
curl -X PUT http://localhost:8083/connectors/patient-sink-postgres/config \
    -H "Content-Type: application/json" \
    -d '{
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": "jdbc:postgresql://172.29.0.1:2000/",
        "connection.user": "postgres",
        "connection.password": "password",
        "tasks.max": "1",
        "topics": "patient",
        "auto.create": "true",
        "auto.evolve":"true",
        "insert.mode": "insert",
        "table.name.format":"patient"
    }'