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
package io.prestosql.plugin.kafka.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.plugin.kafka.KafkaPlugin;
import io.prestosql.plugin.kafka.KafkaQueryRunner;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.MapBasedTableDescriptionSupplier;
import io.prestosql.plugin.kafka.TableDescriptionSupplier;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.kafka.TestingKafka;
import io.prestosql.testing.kafka.TestingKafkaWithSchemaRegistry;

import java.util.HashMap;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.kafka.KafkaPlugin.DEFAULT_EXTENSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class KafkaWithConfluentSchemaRegistryQueryRunner
{
    private KafkaWithConfluentSchemaRegistryQueryRunner() {}

    private static final String DEFAULT_SCHEMA = "default";

    public static KafkaWithConfluentSchemaRegistryQueryRunner.Builder builder(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
    {
        return new KafkaWithConfluentSchemaRegistryQueryRunner.Builder(testingKafkaWithSchemaRegistry);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder
    {
        private final TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;
        private Map<String, String> extraKafkaProperties = ImmutableMap.of();
        private Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription = ImmutableMap.of();
        private Module extension = DEFAULT_EXTENSION;

        protected Builder(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
        {
            super(testSessionBuilder()
                    .setCatalog("kafka")
                    .setSchema(DEFAULT_SCHEMA)
                    .build());
            this.testingKafkaWithSchemaRegistry = requireNonNull(testingKafkaWithSchemaRegistry, "testingKafkaWithSchemaRegistry is null");
        }

        public KafkaWithConfluentSchemaRegistryQueryRunner.Builder setExtraKafkaProperties(Map<String, String> extraKafkaProperties)
        {
            this.extraKafkaProperties = ImmutableMap.copyOf(requireNonNull(extraKafkaProperties, "extraKafkaProperties is null"));
            return this;
        }

        public KafkaWithConfluentSchemaRegistryQueryRunner.Builder setExtraTopicDescription(Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
        {
            this.extraTopicDescription = ImmutableMap.copyOf(requireNonNull(extraTopicDescription, "extraTopicDescription is null"));
            return this;
        }

        public KafkaWithConfluentSchemaRegistryQueryRunner.Builder setExtension(Module extension)
        {
            this.extension = requireNonNull(extension, "extension is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            Logging logging = Logging.initialize();
            logging.setLevel("org.apache.kafka", Level.WARN);
            DistributedQueryRunner queryRunner = super.build();

            return createKafkaQueryRunner(queryRunner, testingKafkaWithSchemaRegistry, extraKafkaProperties, extraTopicDescription, extension);
        }
    }

    private static DistributedQueryRunner createKafkaQueryRunner(
            DistributedQueryRunner queryRunner,
            TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry,
            Map<String, String> extraKafkaProperties,
            Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription,
            Module extensions)
    {
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            testingKafkaWithSchemaRegistry.start();

            KafkaPlugin kafkaPlugin = new KafkaPlugin(combine(
                    extensions,
                    binder -> newSetBinder(binder, TableDescriptionSupplier.class)
                            .addBinding()
                            .toInstance(new MapBasedTableDescriptionSupplier(extraTopicDescription))));
            queryRunner.installPlugin(kafkaPlugin);

            Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
            kafkaProperties.putIfAbsent("kafka.nodes", testingKafkaWithSchemaRegistry.getConnectString());
            kafkaProperties.putIfAbsent("kafka.table-description-suppliers", "CONFLUENT");
            kafkaProperties.putIfAbsent("kafka.confluent-schema-registry-url", testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString());
            kafkaProperties.putIfAbsent("kafka.default-schema", DEFAULT_SCHEMA);
            kafkaProperties.putIfAbsent("kafka.messages-per-split", "1000");
            queryRunner.createCatalog("kafka", "kafka", kafkaProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder(new TestingKafkaWithSchemaRegistry(new TestingKafka()))
                .build();
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
