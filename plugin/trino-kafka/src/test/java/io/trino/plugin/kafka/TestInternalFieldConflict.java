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
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.testng.annotations.Test;

import static io.trino.plugin.kafka.util.TestUtils.createDescription;
import static io.trino.plugin.kafka.util.TestUtils.createOneFieldDescription;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;

@Test(singleThreaded = true)
public class TestInternalFieldConflict
        extends AbstractTestQueryFramework
{
    private SchemaTableName topicWithDefaultPrefixes;
    private SchemaTableName topicWithCustomPrefixes;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka testingKafka = closeAfterClass(TestingKafka.create());
        topicWithDefaultPrefixes = new SchemaTableName("default", "test_" + randomNameSuffix());
        topicWithCustomPrefixes = new SchemaTableName("default", "test_" + randomNameSuffix());
        return KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.of(
                        topicWithDefaultPrefixes, createDescription(
                                topicWithDefaultPrefixes,
                                createOneFieldDescription("_key", createVarcharType(15)),
                                ImmutableList.of(createOneFieldDescription("custkey", BIGINT), createOneFieldDescription("acctbal", DOUBLE))),
                        topicWithCustomPrefixes, createDescription(
                                topicWithCustomPrefixes,
                                createOneFieldDescription("unpredictable_prefix_key", createVarcharType(15)),
                                ImmutableList.of(createOneFieldDescription("custkey", BIGINT), createOneFieldDescription("acctbal", DOUBLE)))))
                .setExtraKafkaProperties(ImmutableMap.of("kafka.internal-column-prefix", "unpredictable_prefix_"))
                .build();
    }

    @Test
    public void testInternalFieldPrefix()
    {
        assertQuery("SELECT count(*) FROM " + topicWithDefaultPrefixes, "VALUES 0");
        assertQueryFails("SELECT count(*) FROM " + topicWithCustomPrefixes, ""
                + "Internal Kafka column names conflict with column names from the table. "
                + "Consider changing kafka.internal-column-prefix configuration property. "
                + "topic=" + topicWithCustomPrefixes
                + ", Conflicting names=\\[unpredictable_prefix_key]");
    }
}
