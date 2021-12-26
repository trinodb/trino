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
package io.trino.tests.product.sqlserver;

import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SQL_SERVER;
import static io.trino.tests.product.sqlserver.SqlServerTpchTableDefinitions.NATION;
import static io.trino.tests.product.sqlserver.TestConstants.CONNECTOR_NAME;
import static io.trino.tests.product.sqlserver.TestConstants.KEY_SPACE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestInvalidSelect
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(NATION);
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentTable()
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, "bogus");
        assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentSchema()
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, "does_not_exist", "bogus");
        assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("Schema 'does_not_exist' does not exist");
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentColumn()
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, NATION.getName());
        assertQueryFailure(() -> onTrino().executeQuery(format("SELECT bogus FROM %s", tableName)))
                .hasMessageContaining("Column 'bogus' cannot be resolved");
    }
}
