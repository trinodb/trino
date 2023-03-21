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
package io.trino.plugin.snowflake;

import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeTest;

public class TestSnowflakeDistributedJoinQueries
        extends AbstractTestJoinQueries
{
    private DistributedQueryRunner queryRunner;

    @BeforeTest
    public void copyTables()
            throws Exception
    {
        AbstractTestBase.copyTables(snowflakeRunner());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return snowflakeRunner();
    }

    DistributedQueryRunner snowflakeRunner()
            throws Exception
    {
        if (queryRunner == null) {
            queryRunner = SnowflakeSqlQueryRunner
                    .createSnowflakeSqlQueryRunner(SnowflakeSqlQueryRunner.getCatalog(),
                            SnowflakeSqlQueryRunner.getWarehouse());
            return queryRunner;
        }
        else {
            return queryRunner;
        }
    }
}
