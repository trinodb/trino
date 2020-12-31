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

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.kafka.KafkaQueryRunnerBuilder;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.kafka.TestingKafkaWithSchemaRegistry;

import java.util.HashMap;
import java.util.Map;

public final class KafkaWithConfluentSchemaRegistryQueryRunner
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
