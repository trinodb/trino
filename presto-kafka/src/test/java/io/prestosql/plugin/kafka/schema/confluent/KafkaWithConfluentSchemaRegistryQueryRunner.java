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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.plugin.kafka.KafkaPlugin;
import io.prestosql.plugin.kafka.KafkaQueryRunnerBuilder;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.kafka.TestingKafkaWithSchemaRegistry;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;

public class KafkaWithConfluentSchemaRegistryQueryRunner
{
    private KafkaWithConfluentSchemaRegistryQueryRunner() {}

    private static final String DEFAULT_SCHEMA = "default";

    public static KafkaWithConfluentSchemaRegistryQueryRunner.Builder builder(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
    {
        return new KafkaWithConfluentSchemaRegistryQueryRunner.Builder(testingKafkaWithSchemaRegistry);
    }

    public static class Builder
            extends KafkaQueryRunnerBuilder<TestingKafkaWithSchemaRegistry>
    {
        protected Builder(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
        {
            super(testingKafkaWithSchemaRegistry, DEFAULT_SCHEMA);
        }

        @Override
        public void preInit(DistributedQueryRunner queryRunner)
        {
            Map<String, String> properties = new HashMap<>(extraKafkaProperties);
            properties.putIfAbsent("kafka.table-description-supplier", "confluent");
            properties.putIfAbsent("kafka.confluent-schema-registry-url", testingKafka.getSchemaRegistryConnectString());
            setExtraKafkaProperties(properties);
        }
    }

    private static DistributedQueryRunner createKafkaQueryRunner(
            DistributedQueryRunner queryRunner,
            TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry,
            Map<String, String> extraKafkaProperties,
            Module extensions)
    {
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            testingKafkaWithSchemaRegistry.start();

            KafkaPlugin kafkaPlugin = new KafkaPlugin(extensions);
            queryRunner.installPlugin(kafkaPlugin);

            Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
            kafkaProperties.putIfAbsent("kafka.nodes", testingKafkaWithSchemaRegistry.getConnectString());
            kafkaProperties.putIfAbsent("kafka.table-description-supplier", "confluent");
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
        DistributedQueryRunner queryRunner = builder(new TestingKafkaWithSchemaRegistry())
                .build();
        Logger log = Logger.get(KafkaWithConfluentSchemaRegistryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
