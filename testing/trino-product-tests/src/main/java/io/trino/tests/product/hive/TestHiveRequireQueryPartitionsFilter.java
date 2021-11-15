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
package io.trino.tests.product.hive;

import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static java.lang.String.format;

public class TestHiveRequireQueryPartitionsFilter
        extends ProductTest
        implements RequirementsProvider
{
    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return mutableTable(NATION_PARTITIONED_BY_BIGINT_REGIONKEY, "test_table", LOADED);
    }

    @Test
    public void testRequiresQueryPartitionFilter()
    {
        String tableName = tablesState.get("test_table").getNameInDatabase();

        query("SET SESSION hive.query_partition_filter_required = true");
        assertQueryFailure(() -> query("SELECT COUNT(*) FROM " + tableName))
                .hasMessageMatching(format("Query failed \\(#\\w+\\): Filter required on default\\.%s for at least one partition column: p_regionkey", tableName));
        assertThat(query(format("SELECT COUNT(*) FROM %s WHERE p_regionkey = 1", tableName))).containsOnly(row(5));
    }

    @Test(dataProvider = "queryPartitionFilterRequiredSchemasDataProvider")
    public void testRequiresQueryPartitionFilterOnSpecificSchema(String queryPartitionFilterRequiredSchemas)
    {
        String tableName = tablesState.get("test_table").getNameInDatabase();

        query("SET SESSION hive.query_partition_filter_required = true");
        query(format("SET SESSION hive.query_partition_filter_required_schemas = %s", queryPartitionFilterRequiredSchemas));

        assertQueryFailure(() -> query("SELECT COUNT(*) FROM " + tableName))
                .hasMessageMatching(format("Query failed \\(#\\w+\\): Filter required on default\\.%s for at least one partition column: p_regionkey", tableName));
        assertThat(query(format("SELECT COUNT(*) FROM %s WHERE p_regionkey = 1", tableName))).containsOnly(row(5));
    }

    @DataProvider
    public Object[][] queryPartitionFilterRequiredSchemasDataProvider()
    {
        return new Object[][]{
                {"ARRAY['default']"},
                {"ARRAY['DEFAULT']"},
                {"ARRAY['deFAUlt']"}
        };
    }
}
