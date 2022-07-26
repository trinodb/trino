#!/usr/bin/env bash

set -xeuo pipefail

# Start the topology as defined in https://debezium.io/documentation/reference/stable/tutorial.html
export DEBEZIUM_VERSION=1.9
docker-compose -f debezium-mysql.yaml up -d
sleep 30

# Modify records in the database via MySQL client
docker-compose -f debezium-mysql.yaml exec mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD --execute="USE inventory;
CREATE TABLE addr (
  id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  street VARCHAR(20));
INSERT INTO addr VALUES(1, '\''Otwocka'\'');
INSERT INTO addr VALUES(2, '\''Marszalkowska'\'');

CREATE TABLE users (
  id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(20),
  addr_id INT(6) UNSIGNED,
  CONSTRAINT fk_addr FOREIGN KEY (addr_id) REFERENCES addr(id));
INSERT INTO users VALUES(1, '\''Karol'\'', 1);
INSERT INTO users VALUES(2, '\''Marek'\'', 2);"'

# register mysql connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "inventory", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.inventory" } }'
