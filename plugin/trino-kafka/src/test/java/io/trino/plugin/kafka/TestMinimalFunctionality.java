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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.BasicTestingKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.UUID;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private BasicTestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new BasicTestingKafka();
        topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build())
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.messages-per-split", "100")
                        .build())
                .build();
        testingKafka.createTopic(topicName);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
            throws Exception
    {
        if (testingKafka != null) {
            testingKafka.close();
            testingKafka = null;
        }
    }

    @Test
    public void testTopicExists()
    {
        assertTrue(getQueryRunner().listTables(getSession(), "kafka", "default").contains(QualifiedObjectName.valueOf("kafka.default." + topicName)));
    }

    @Test
    public void testTopicHasData()
    {
        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 0");

        createMessages(topicName);

        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 100000L");
    }

    private void createMessages(String topicName)
    {
        try (KafkaProducer<Long, Object> producer = testingKafka.createProducer()) {
            int jMax = 10_000;
            int iMax = 100_000 / jMax;
            for (long i = 0; i < iMax; i++) {
                for (long j = 0; j < jMax; j++) {
                    producer.send(new ProducerRecord<>(topicName, i, ImmutableMap.of("id", Long.toString(i * iMax + j), "value", UUID.randomUUID().toString())));
                }
            }
        }
    }
}
