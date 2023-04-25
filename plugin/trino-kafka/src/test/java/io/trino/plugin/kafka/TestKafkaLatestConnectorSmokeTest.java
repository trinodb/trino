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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import org.testng.SkipException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaLatestConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka testingKafka = closeAfterClass(TestingKafka.create("7.1.1"));
        return KafkaQueryRunner.builder(testingKafka)
                .setTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessage("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        // TODO implement the test for Kafka
        throw new SkipException("TODO");
    }
}
