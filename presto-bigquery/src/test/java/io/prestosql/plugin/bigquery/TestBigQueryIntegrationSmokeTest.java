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
package io.prestosql.plugin.bigquery;

import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

@Test
public class TestBigQueryIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("The BigQuery connector is read only");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner();
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

    @Test
    @Override
    public void testDuplicatedRowCreateTable()
    {
        throw new SkipException("The BigQuery connector is read only");
    }

    @Override
    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Override
    protected MaterializedResult getExpectedOrdersTableDescription(boolean dateSupported, boolean parametrizedVarchar)
    {
        return MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }
}
