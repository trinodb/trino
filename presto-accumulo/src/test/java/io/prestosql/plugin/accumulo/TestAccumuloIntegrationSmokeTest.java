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
package io.prestosql.plugin.accumulo;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;

import static io.prestosql.plugin.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static org.testng.Assert.assertEquals;

public class TestAccumuloIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createAccumuloQueryRunner(ImmutableMap.of());
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

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Accumulo connector does not support column default values");
    }

    @Override
    public void testDescribeTable()
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actual = computeActual("DESC ORDERS").toTestTypes();
        assertEquals(actual.getMaterializedRows().get(0).getField(0), "orderkey");
        assertEquals(actual.getMaterializedRows().get(0).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(1).getField(0), "custkey");
        assertEquals(actual.getMaterializedRows().get(1).getField(1), "bigint");
        assertEquals(actual.getMaterializedRows().get(2).getField(0), "orderstatus");
        assertEquals(actual.getMaterializedRows().get(2).getField(1), "varchar(1)");
        assertEquals(actual.getMaterializedRows().get(3).getField(0), "totalprice");
        assertEquals(actual.getMaterializedRows().get(3).getField(1), "double");
        assertEquals(actual.getMaterializedRows().get(4).getField(0), "orderdate");
        assertEquals(actual.getMaterializedRows().get(4).getField(1), "date");
        assertEquals(actual.getMaterializedRows().get(5).getField(0), "orderpriority");
        assertEquals(actual.getMaterializedRows().get(5).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(6).getField(0), "clerk");
        assertEquals(actual.getMaterializedRows().get(6).getField(1), "varchar(15)");
        assertEquals(actual.getMaterializedRows().get(7).getField(0), "shippriority");
        assertEquals(actual.getMaterializedRows().get(7).getField(1), "integer");
        assertEquals(actual.getMaterializedRows().get(8).getField(0), "comment");
        assertEquals(actual.getMaterializedRows().get(8).getField(1), "varchar(79)");
    }
}
