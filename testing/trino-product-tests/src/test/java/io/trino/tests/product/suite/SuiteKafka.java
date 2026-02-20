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
package io.trino.tests.product.suite;

import io.trino.tests.product.TestGroup;
import io.trino.tests.product.kafka.KafkaBasicEnvironment;
import io.trino.tests.product.kafka.KafkaConfluentBasicEnvironment;
import io.trino.tests.product.kafka.KafkaSaslPlaintextEnvironment;
import io.trino.tests.product.kafka.KafkaSchemaRegistryEnvironment;
import io.trino.tests.product.kafka.KafkaSslEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 test suite for Kafka connector tests.
 * <p>
 * This suite runs two sequential test runs:
 * <ol>
 *   <li>Kafka basic tests: All tests tagged with {@code kafka} but not {@code kafka_schema_registry}</li>
 *   <li>Kafka schema registry tests: All tests tagged with {@code kafka_schema_registry} or {@code kafka_confluent_license}</li>
 * </ol>
 * <p>
 * To run this suite:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.trino.tests.product.suite.SuiteKafka"
 * </pre>
 */
public final class SuiteKafka
{
    private SuiteKafka() {}

    public static void main(String[] args)
    {
        // Set strict mode to ensure tests are properly isolated
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        // Run 1: Kafka basic tests (excluding schema registry/confluent runs)
        results.add(SuiteRunner.forEnvironment(KafkaBasicEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaSchemaRegistry.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        results.add(SuiteRunner.forEnvironment(KafkaConfluentBasicEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaSchemaRegistry.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        results.add(SuiteRunner.forEnvironment(KafkaSslEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaSchemaRegistry.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        results.add(SuiteRunner.forEnvironment(KafkaSaslPlaintextEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaSchemaRegistry.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Run 2: Kafka schema registry tests
        results.add(SuiteRunner.forEnvironment(KafkaSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.KafkaSchemaRegistry.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Run 3: Confluent-licensed protobuf schema-registry tests
        results.add(SuiteRunner.forEnvironment(KafkaSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Print combined summary
        SuiteRunner.printSummary(results);

        // Exit with appropriate code
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
