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
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private TestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        topicName = "test_" + randomUUID().toString().replaceAll("-", "_");
        SchemaTableName schemaTableName = new SchemaTableName("default", topicName);
        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.of(schemaTableName, createEmptyTopicDescription(topicName, schemaTableName).getValue()))
                .setExtraKafkaProperties(ImmutableMap.of("kafka.messages-per-split", "100"))
                .build();
        return queryRunner;
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

        testingKafka.sendMessages(LongStream.range(0, 100000)
                .mapToObj(id -> new ProducerRecord<>(topicName, id, ImmutableMap.of("id", Long.toString(id), "value", randomUUID().toString()))));

        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 100000L");
    }
}
