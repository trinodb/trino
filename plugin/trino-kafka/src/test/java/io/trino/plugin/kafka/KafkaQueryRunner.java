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
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.kafka.encoder.EncoderModule;
import io.trino.plugin.kafka.schema.ContentSchemaReader;
import io.trino.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.plugin.kafka.schema.file.FileContentSchemaReader;
import io.trino.plugin.kafka.util.CodecSupplier;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.ByteStreams.toByteArray;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.kafka.util.TestUtils.loadTpchTopicDescription;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner() {}

    private static final Logger log = Logger.get(KafkaQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";
    private static final String TEST = "test";

    public static Builder builder(TestingKafka testingKafka)
    {
        return new Builder(testingKafka);
    }

    public static class Builder
            extends KafkaQueryRunnerBuilder
    {
        private List<TpchTable<?>> tables = ImmutableList.of();
        private Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription = ImmutableMap.of();

        protected Builder(TestingKafka testingKafka)
        {
            super(testingKafka, TPCH_SCHEMA);
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        public Builder setExtraTopicDescription(Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
        {
            this.extraTopicDescription = ImmutableMap.copyOf(requireNonNull(extraTopicDescription, "extraTopicDescription is null"));
            return this;
        }

        @Override
        public Builder setExtension(Module extension)
        {
            this.extension = requireNonNull(extension, "extension is null");
            return this;
        }

        @Override
        public void preInit(DistributedQueryRunner queryRunner)
                throws Exception
        {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            Map<SchemaTableName, KafkaTopicDescription> tpchTopicDescriptions = createTpchTopicDescriptions(queryRunner.getCoordinator().getTypeManager(), tables);

            List<SchemaTableName> tableNames = new ArrayList<>();
            tableNames.add(new SchemaTableName("read_test", "all_datatypes_json"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_avro"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_csv"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_raw"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_json"));

            JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, queryRunner.getCoordinator().getTypeManager()).get();

            ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> testTopicDescriptions = ImmutableMap.builder();
            for (SchemaTableName tableName : tableNames) {
                testTopicDescriptions.put(tableName, createTable(tableName, topicDescriptionJsonCodec));
            }

            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                    .putAll(extraTopicDescription)
                    .putAll(tpchTopicDescriptions)
                    .putAll(testTopicDescriptions.buildOrThrow())
                    .buildOrThrow();
            setExtension(combine(
                    extension,
                    conditionalModule(
                            KafkaConfig.class,
                            kafkaConfig -> kafkaConfig.getTableDescriptionSupplier().equalsIgnoreCase(TEST),
                            binder -> binder.bind(TableDescriptionSupplier.class)
                                    .toInstance(new MapBasedTableDescriptionSupplier(topicDescriptions))),
                    binder -> binder.bind(ContentSchemaReader.class).to(FileContentSchemaReader.class).in(Scopes.SINGLETON),
                    new DecoderModule(),
                    new EncoderModule()));
            Map<String, String> properties = new HashMap<>(extraKafkaProperties);
            properties.putIfAbsent("kafka.table-description-supplier", TEST);
            setExtraKafkaProperties(properties);
        }

        @Override
        public void postInit(DistributedQueryRunner queryRunner)
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
    }

    private static KafkaTopicDescription createTable(SchemaTableName table, JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec)
            throws IOException
    {
        String fileName = format("/%s/%s.json", table.getSchemaName(), table.getTableName());
        KafkaTopicDescription tableTemplate = topicDescriptionJsonCodec.fromJson(toByteArray(KafkaQueryRunner.class.getResourceAsStream(fileName)));

        Optional<KafkaTopicFieldGroup> key = tableTemplate.getKey()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.getFields()));

        Optional<KafkaTopicFieldGroup> message = tableTemplate.getMessage()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.getFields()));

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

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder(TestingKafka.create())
                .setTables(TpchTable.getTables())
                .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
