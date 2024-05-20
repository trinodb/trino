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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.kafka.encoder.EncoderModule;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.plugin.kafka.schema.file.FileReadContentSchemaProvider;
import io.trino.plugin.kafka.util.CodecSupplier;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.kafka.util.TestUtils.loadTpchTopicDescription;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.OFF);
    }

    private static final Logger log = Logger.get(KafkaQueryRunner.class);

    private static final String TPCH_SCHEMA = "tpch";
    private static final String TEST = "test";

    public static Builder builder(TestingKafka testingKafka)
    {
        return new Builder(TPCH_SCHEMA, false)
                .addConnectorProperties(Map.of(
                        "kafka.nodes", testingKafka.getConnectString(),
                        "kafka.messages-per-split", "1000",
                        "kafka.table-description-supplier", TEST));
    }

    public static Builder builderForConfluentSchemaRegistry(TestingKafka testingKafka)
    {
        return new Builder("default", true)
                .addConnectorProperties(Map.of(
                        "kafka.nodes", testingKafka.getConnectString(),
                        "kafka.messages-per-split", "1000",
                        "kafka.table-description-supplier", "confluent",
                        "kafka.confluent-schema-registry-url", testingKafka.getSchemaRegistryConnectString(),
                        "kafka.protobuf-any-support-enabled", "true"));
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> tables = ImmutableList.of();
        private Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription = ImmutableMap.of();
        private final boolean schemaRegistryEnabled;

        private Builder(String schemaName, boolean schemaRegistryEnabled)
        {
            super(testSessionBuilder()
                    .setCatalog("kafka")
                    .setSchema(schemaName)
                    .build());
            this.schemaRegistryEnabled = schemaRegistryEnabled;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setExtraTopicDescription(Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
        {
            this.extraTopicDescription = ImmutableMap.copyOf(requireNonNull(extraTopicDescription, "extraTopicDescription is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                ImmutableList.Builder<Module> extensions = ImmutableList.<Module>builder();

                if (schemaRegistryEnabled) {
                    checkState(extraTopicDescription.isEmpty(), "unsupported extraTopicDescription with schema registry enabled");
                }
                else {
                    ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                            .putAll(extraTopicDescription)
                            .putAll(createTpchTopicDescriptions(queryRunner.getPlannerContext().getTypeManager(), tables));

                    List<SchemaTableName> tableNames = new ArrayList<>();
                    tableNames.add(new SchemaTableName("read_test", "all_datatypes_json"));
                    tableNames.add(new SchemaTableName("write_test", "all_datatypes_avro"));
                    tableNames.add(new SchemaTableName("write_test", "all_datatypes_csv"));
                    tableNames.add(new SchemaTableName("write_test", "all_datatypes_raw"));
                    tableNames.add(new SchemaTableName("write_test", "all_datatypes_json"));
                    JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, queryRunner.getPlannerContext().getTypeManager()).get();
                    for (SchemaTableName tableName : tableNames) {
                        topicDescriptions.put(tableName, createTable(tableName, topicDescriptionJsonCodec));
                    }

                    extensions
                            .add(conditionalModule(
                                    KafkaConfig.class,
                                    kafkaConfig -> kafkaConfig.getTableDescriptionSupplier().equalsIgnoreCase(TEST),
                                    binder -> binder.bind(TableDescriptionSupplier.class)
                                            .toInstance(new MapBasedTableDescriptionSupplier(topicDescriptions.buildOrThrow()))))
                            .add(binder -> binder.bind(ContentSchemaProvider.class).to(FileReadContentSchemaProvider.class).in(Scopes.SINGLETON))
                            .add(new DecoderModule())
                            .add(new EncoderModule());
                }

                queryRunner.installPlugin(new KafkaPlugin(extensions.build()));
                queryRunner.createCatalog("kafka", "kafka", connectorProperties);

                if (schemaRegistryEnabled) {
                    checkState(tables.isEmpty(), "unsupported tables with schema registry enabled");
                }
                else {
                    populateTables(queryRunner, tables);
                }

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void populateTables(QueryRunner queryRunner, List<TpchTable<?>> tables)
    {
        log.info("Loading data...");
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            long start = System.nanoTime();
            log.info("Running import for %s", table.getTableName());
            queryRunner.execute(format("INSERT INTO %1$s SELECT * FROM tpch.tiny.%1$s", table.getTableName()));
            log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
        }
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
    }

    private static KafkaTopicDescription createTable(SchemaTableName table, JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec)
            throws IOException
    {
        String fileName = format("/%s/%s.json", table.getSchemaName(), table.getTableName());
        KafkaTopicDescription tableTemplate = topicDescriptionJsonCodec.fromJson(toByteArray(KafkaQueryRunner.class.getResourceAsStream(fileName)));

        Optional<KafkaTopicFieldGroup> key = tableTemplate.key()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.dataFormat(),
                        keyTemplate.dataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.fields()));

        Optional<KafkaTopicFieldGroup> message = tableTemplate.message()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.dataFormat(),
                        keyTemplate.dataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.fields()));

        return new KafkaTopicDescription(
                table.getTableName(),
                Optional.of(table.getSchemaName()),
                table.toString(),
                key,
                message);
    }

    private static Map<SchemaTableName, KafkaTopicDescription> createTpchTopicDescriptions(TypeManager typeManager, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, typeManager).get();

        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            topicDescriptions.put(loadTpchTopicDescription(topicDescriptionJsonCodec, tpchTable.toString(), tpchTable));
        }
        return topicDescriptions.buildOrThrow();
    }

    public static final class DefaultKafkaQueryRunnerMain
    {
        private DefaultKafkaQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            TestingKafka testingKafka = TestingKafka.create();
            testingKafka.start();
            QueryRunner queryRunner = builder(testingKafka)
                    .setTables(TpchTable.getTables())
                    .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .build();
            Logger log = Logger.get(KafkaQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class ConfluentSchemaRegistryQueryRunnerMain
    {
        private ConfluentSchemaRegistryQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            TestingKafka testingKafka = TestingKafka.createWithSchemaRegistry();
            testingKafka.start();
            QueryRunner queryRunner = builderForConfluentSchemaRegistry(testingKafka)
                    .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .build();
            Logger log = Logger.get(KafkaQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
