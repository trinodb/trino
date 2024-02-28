/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

import static io.trino.plugin.pinot.BasePinotConnectorSmokeTest.schemaRegistryAwareProducer;
import static java.time.temporal.ChronoUnit.SECONDS;

public final class PinotTpchTables
{
    // Use a recent value for updated_at to ensure Pinot doesn't clean up records older than retentionTimeValue as defined in the table specs
    private static final Instant INITIAL_UPDATED_AT = Instant.now().minus(Duration.ofDays(1)).truncatedTo(SECONDS);

    private PinotTpchTables() {}

    public static void createTpchTables(TestingKafka kafka, TestingPinotCluster pinot, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        for (TpchTable<?> table : tables) {
            switch (table.getTableName()) {
                case "region" -> createRegionTable(kafka, pinot, queryRunner);
                case "nation" -> createNationTable(kafka, pinot, queryRunner);
                case "orders" -> createOrdersTable(kafka, pinot, queryRunner);
                case "customer" -> createCustomerTable(kafka, pinot, queryRunner);
            }
        }
    }

    private static void createRegionTable(TestingKafka kafka, TestingPinotCluster pinot, QueryRunner queryRunner)
            throws Exception
    {
        String regionTableName = "region";
        kafka.createTopicWithConfig(2, 1, regionTableName, false);
        Schema regionSchema = SchemaBuilder.record(regionTableName).fields()
                .name("regionkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> regionRowsBuilder = ImmutableList.builder();
        MaterializedResult regionRows = queryRunner.execute("SELECT * FROM tpch.tiny.region");
        for (MaterializedRow row : regionRows.getMaterializedRows()) {
            regionRowsBuilder.add(new ProducerRecord<>(regionTableName, "key" + row.getField(0), new GenericRecordBuilder(regionSchema)
                    .set("regionkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(2))
                    .set("updated_at_seconds", INITIAL_UPDATED_AT.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(regionRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("region_schema.json"), regionTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("region_realtimeSpec.json"), regionTableName);
    }

    private static void createNationTable(TestingKafka kafka, TestingPinotCluster pinot, QueryRunner queryRunner)
            throws Exception
    {
        String nationTableName = "nation";
        kafka.createTopicWithConfig(2, 1, nationTableName, false);
        Schema nationSchema = SchemaBuilder.record(nationTableName).fields()
                .name("nationkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("regionkey").type().longType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> nationRowsBuilder = ImmutableList.builder();
        MaterializedResult nationRows = queryRunner.execute("SELECT * FROM tpch.tiny.nation");
        for (MaterializedRow row : nationRows.getMaterializedRows()) {
            nationRowsBuilder.add(new ProducerRecord<>(nationTableName, "key" + row.getField(0), new GenericRecordBuilder(nationSchema)
                    .set("nationkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(3))
                    .set("regionkey", row.getField(2))
                    .set("updated_at_seconds", INITIAL_UPDATED_AT.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(nationRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("nation_schema.json"), nationTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("nation_realtimeSpec.json"), nationTableName);
    }

    private static void createOrdersTable(TestingKafka kafka, TestingPinotCluster pinot, QueryRunner queryRunner)
            throws Exception
    {
        String ordersTableName = "orders";
        kafka.createTopicWithConfig(2, 1, ordersTableName, false);
        Schema ordersSchema = SchemaBuilder.record(ordersTableName).fields()
                .name("orderkey").type().longType().noDefault()
                .name("custkey").type().longType().noDefault()
                .name("orderstatus").type().stringType().noDefault()
                .name("totalprice").type().doubleType().noDefault()
                .name("orderdate").type().longType().noDefault()
                .name("orderpriority").type().stringType().noDefault()
                .name("clerk").type().stringType().noDefault()
                .name("shippriority").type().intType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> ordersRowsBuilder = ImmutableList.builder();
        MaterializedResult ordersRows = queryRunner.execute("SELECT * FROM tpch.tiny.orders");
        for (MaterializedRow row : ordersRows.getMaterializedRows()) {
            ordersRowsBuilder.add(new ProducerRecord<>(ordersTableName, "key" + row.getField(0), new GenericRecordBuilder(ordersSchema)
                    .set("orderkey", row.getField(0))
                    .set("custkey", row.getField(1))
                    .set("orderstatus", row.getField(2))
                    .set("totalprice", row.getField(3))
                    .set("orderdate", LocalDate.parse(row.getField(4).toString()).toEpochDay())
                    .set("orderpriority", row.getField(5))
                    .set("clerk", row.getField(6))
                    .set("shippriority", row.getField(7))
                    .set("comment", row.getField(8))
                    .set("updated_at", INITIAL_UPDATED_AT.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(ordersRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("orders_schema.json"), ordersTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("orders_realtimeSpec.json"), ordersTableName);
    }

    private static void createCustomerTable(TestingKafka kafka, TestingPinotCluster pinot, QueryRunner queryRunner)
            throws Exception
    {
        String customerTableName = "customer";
        kafka.createTopicWithConfig(2, 1, customerTableName, false);
        Schema customerSchema = SchemaBuilder.record(customerTableName).fields()
                .name("custkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("address").type().stringType().noDefault()
                .name("nationkey").type().longType().noDefault()
                .name("phone").type().stringType().noDefault()
                .name("acctbal").type().doubleType().noDefault()
                .name("mktsegment").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> customerRowsBuilder = ImmutableList.builder();
        MaterializedResult customerRows = queryRunner.execute("SELECT * FROM tpch.tiny.customer");
        for (MaterializedRow row : customerRows.getMaterializedRows()) {
            customerRowsBuilder.add(new ProducerRecord<>(customerTableName, "key" + row.getField(0), new GenericRecordBuilder(customerSchema)
                    .set("custkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("address", row.getField(2))
                    .set("nationkey", row.getField(3))
                    .set("phone", row.getField(4))
                    .set("acctbal", row.getField(5))
                    .set("mktsegment", row.getField(6))
                    .set("comment", row.getField(7))
                    .set("updated_at", INITIAL_UPDATED_AT.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(customerRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("customer_schema.json"), customerTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("customer_realtimeSpec.json"), customerTableName);
    }
}
