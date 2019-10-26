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
package io.prestosql.plugin.kafka;

import io.airlift.tpch.TpchTable;
import io.prestosql.plugin.kafka.util.TestingKafka;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import static io.prestosql.plugin.kafka.KafkaQueryRunner.createKafkaQueryRunner;

public class TestKafkaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        return createKafkaQueryRunner(testingKafka, TpchTable.getTables());
    }

    @Override
    protected boolean canCreateSchema()
    {
        return false;
    }

    @Override
    protected boolean canDropSchema()
    {
        return false;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        testingKafka.close();
    }
}
