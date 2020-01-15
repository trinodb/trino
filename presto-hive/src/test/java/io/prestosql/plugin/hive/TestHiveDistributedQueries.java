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
package io.prestosql.plugin.hive;

import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.prestosql.tpch.TpchTable.getTables;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(getTables())
                .build();
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Hive connector does not support column default values");
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return true;
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Override
    public void testColumnName(String columnName)
    {
        if (columnName.equals("atrailingspace ") || columnName.equals(" aleadingspace")) {
            // TODO (https://github.com/prestosql/presto/issues/3461)
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasMessageMatching("Table '.*' does not have columns \\[" + columnName + "]");
            throw new SkipException("works incorrectly, column name is trimmed");
        }
        if (columnName.equals("a,comma")) {
            // TODO (https://github.com/prestosql/presto/issues/3537)
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasMessageMatching("Table '.*' does not have columns \\[a,comma]");
            throw new SkipException("works incorrectly");
        }

        super.testColumnName(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
