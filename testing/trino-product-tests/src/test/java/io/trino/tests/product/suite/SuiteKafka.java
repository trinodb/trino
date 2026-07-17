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
import io.trino.tests.product.kafka.KafkaConfluentSchemaRegistryEnvironment;
import io.trino.tests.product.kafka.KafkaSaslPlaintextSchemaRegistryEnvironment;
import io.trino.tests.product.kafka.KafkaSchemaRegistryEnvironment;
import io.trino.tests.product.kafka.KafkaSslSchemaRegistryEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 test suite for Kafka connector tests.
 * <p>
 * This suite runs four sequential test runs:
 * <ol>
 *   <li>Kafka and Schema Registry tests against stock Trino</li>
 *   <li>Kafka and Schema Registry tests with Confluent-licensed Protobuf support</li>
 *   <li>Kafka and Schema Registry tests over SSL</li>
 *   <li>Kafka and Schema Registry tests over SASL/PLAIN</li>
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
            throws Exception
    {
        // Set strict mode to ensure tests are properly isolated
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        // Run 1: Kafka and Schema Registry tests against stock Trino
        results.add(SuiteRunner.forEnvironment(KafkaSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Run 2: Kafka and Schema Registry tests with Confluent-licensed Protobuf support
        results.add(SuiteRunner.forEnvironment(KafkaConfluentSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .run());

        // Run 3: Kafka and Schema Registry tests over SSL
        results.add(SuiteRunner.forEnvironment(KafkaSslSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Run 4: Kafka and Schema Registry tests over SASL/PLAIN
        results.add(SuiteRunner.forEnvironment(KafkaSaslPlaintextSchemaRegistryEnvironment.class)
                .includeTag(TestGroup.Kafka.class)
                .excludeTag(TestGroup.KafkaConfluentLicense.class)
                .run());

        // Print combined summary
        SuiteRunner.printSummary(results);

        // Exit with appropriate code
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
