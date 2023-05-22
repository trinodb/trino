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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.kafka.TestingKafka;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.pinot.BasePinotConnectorSmokeTest.schemaRegistryAwareProducer;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.temporal.ChronoUnit.DAYS;

public class PinotQueryRunner
{
    public static final String PINOT_CATALOG = "pinot";

    private PinotQueryRunner() {}

    public static DistributedQueryRunner createPinotQueryRunner(Map<String, String> extraProperties, Map<String, String> extraPinotProperties, Optional<Module> extension)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("default"))
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new PinotPlugin(extension));
        queryRunner.createCatalog(PINOT_CATALOG, "pinot", extraPinotProperties);
        return queryRunner;
    }

    public static Session createSession(String schema)
    {
        return createSession(schema, new PinotConfig());
    }

    public static Session createSession(String schema, PinotConfig config)
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(config);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(
                ImmutableSet.of(new SystemSessionProperties()),
                CatalogServiceProvider.singleton(createTestCatalogHandle(PINOT_CATALOG), Maps.uniqueIndex(pinotSessionProperties.getSessionProperties(), PropertyMetadata::getName)));
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog(PINOT_CATALOG)
                .setSchema(schema)
                .build();
    }

    public static void createTpchTables(TestingKafka kafka, TestingPinotCluster pinot, DistributedQueryRunner queryRunner, Instant initialUpdatedAt)
            throws Exception
    {
        // Create and populate table and topic data
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
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(regionRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("region_schema.json"), regionTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("region_realtimeSpec.json"), regionTableName);

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
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(nationRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("nation_schema.json"), nationTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("nation_realtimeSpec.json"), nationTableName);

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
                    .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(ordersRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("orders_schema.json"), ordersTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("orders_realtimeSpec.json"), ordersTableName);

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
                    .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(customerRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(PinotQueryRunner.class.getClassLoader().getResourceAsStream("customer_schema.json"), customerTableName);
        pinot.addRealTimeTable(PinotQueryRunner.class.getClassLoader().getResourceAsStream("customer_realtimeSpec.json"), customerTableName);
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingKafka kafka = TestingKafka.createWithSchemaRegistry();
        kafka.start();
        TestingPinotCluster pinot = new TestingPinotCluster(kafka.getNetwork(), false, PINOT_LATEST_IMAGE_NAME);
        pinot.start();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                .put("pinot.segments-per-split", "10")
                .buildOrThrow();
        DistributedQueryRunner queryRunner = createPinotQueryRunner(
                properties,
                pinotProperties,
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort(), pinot.getServerGrpcHostAndPort()))));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        createTpchTables(kafka, pinot, queryRunner, Instant.now().minus(Duration.ofDays(1)).truncatedTo(DAYS));
        Thread.sleep(10);
        Logger log = Logger.get(PinotQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
