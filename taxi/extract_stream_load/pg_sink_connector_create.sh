#!/bin/bash

gateway=$(docker inspect postgresql -f "{{json .NetworkSettings.Networks }}" | jq -r '.taxi_default."Gateway"')

read -p "Enter the topic name: " topic
read -p "Enter the destination table name: " table

curl_cmd='curl -X PUT http://localhost:8083/connectors/taxi-sink-pg/config 
    -H "Content-Type: application/json" 
    -d '\''{
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": "jdbc:postgresql://'$gateway':2000/",
        "connection.user": "postgres",
        "connection.password": "password",
        "tasks.max": "1",
        "topics": "'$topic'",
        "auto.create": "true",
        "auto.evolve":"true",
        "insert.mode": "insert",
        "table.name.format":"'$table'"
    }'\'''

echo $curl_cmd
eval $curl_cmd
