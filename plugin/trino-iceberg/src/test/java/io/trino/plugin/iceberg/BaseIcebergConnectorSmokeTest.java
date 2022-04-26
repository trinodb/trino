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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;

    public BaseIcebergConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return true;

            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        format("   location = '.*/" + schemaName + "/region'\n") +
                        "\\)");
    }

    @Test
    public void testHiddenPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }
}
