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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.KafkaConnectorFactory;
import io.trino.plugin.kafka.KafkaQueryRunnerBuilder;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchTable;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.kafka.schema.confluent.ConfluentModule.createSchemaRegistryClient;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public final class KafkaWithConfluentSchemaRegistryQueryRunner
{
    private static final Logger log = Logger.get(KafkaWithConfluentSchemaRegistryQueryRunner.class);

    private KafkaWithConfluentSchemaRegistryQueryRunner() {}

    private static final String DEFAULT_SCHEMA = "default";

    public static KafkaWithConfluentSchemaRegistryQueryRunner.Builder builder(TestingKafka testingKafka)
    {
        return new KafkaWithConfluentSchemaRegistryQueryRunner.Builder(testingKafka);
    }

    public static class Builder
            extends KafkaQueryRunnerBuilder
    {
        private List<TpchTable<?>> tables = ImmutableList.of();

        protected Builder(TestingKafka testingKafka)
        {
            super(testingKafka, DEFAULT_SCHEMA);
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        @Override
        public void preInit(DistributedQueryRunner queryRunner)
        {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            Map<String, String> properties = new HashMap<>(extraKafkaProperties);
            properties.putIfAbsent("kafka.table-description-supplier", "confluent");
            properties.putIfAbsent("kafka.confluent-schema-registry-url", testingKafka.getSchemaRegistryConnectString());
            properties.putIfAbsent("kafka.protobuf-any-support-enabled", "true");
            setExtraKafkaProperties(properties);
        }

        @Override
        public void postInit(DistributedQueryRunner queryRunner)
        {
            log.info("Loading data...");
            long startTime = System.nanoTime();
            SchemaRegistryClient schemaRegistryClient = createSchemaRegistryClient(new ConfluentSchemaRegistryConfig().setConfluentSchemaRegistryUrls(testingKafka.getSchemaRegistryConnectString()),
                    Set.of(),
                    Set.of(),
                    KafkaConnectorFactory.class.getClassLoader());
            for (TpchTable<?> table : tables) {
                long start = System.nanoTime();
                log.info("Running import for %s", table.getTableName());
                createSubjectForTpchTable(table, schemaRegistryClient);
                String columnList = table.getColumns().stream()
                        .map(TpchColumn::getSimplifiedColumnName)
                        .collect(joining(","));
                queryRunner.execute("""
                    INSERT INTO %1$s (%2$s)
                    SELECT %2$s
                    FROM tpch.tiny.%1$s""".formatted(table.getTableName(), columnList));
                log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
        }

        private void createSubjectForTpchTable(TpchTable<?> table, SchemaRegistryClient schemaRegistryClient)
        {
            try {
                AvroSchema schema = new AvroSchema(new Schema.Parser().parse(Resources.toString(Resources.getResource(KafkaWithConfluentSchemaRegistryQueryRunner.class, "/tpch_avro/%s.avsc".formatted(table.getTableName())), UTF_8)));
                schemaRegistryClient.register("%s-value".formatted(table.getTableName()), schema);
            }
            catch (RestClientException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder(TestingKafka.createWithSchemaRegistry())
                .build();
        Logger log = Logger.get(KafkaWithConfluentSchemaRegistryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
