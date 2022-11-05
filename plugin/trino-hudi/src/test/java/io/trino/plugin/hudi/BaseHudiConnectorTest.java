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
package io.trino.plugin.hudi;

import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseHudiConnectorTest
        extends BaseConnectorTest
{
    public static final String SCHEMA_NAME = "tests";

    private final HudiTableType tableType;

    public BaseHudiConnectorTest(HudiTableType tableType)
    {
        this.tableType = requireNonNull(tableType, "table type is null");
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_INSERT:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .matches("\\QCREATE TABLE hudi." + schema + ".orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/orders'\n\\Q" +
                        ")");
    }

    @Test
    public void testHideHiveSysSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain("sys");
        assertQueryFails("SHOW TABLES IN hudi.sys", ".*Schema 'sys' does not exist");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomTableSuffix();
        String roTableName = tableName + "_ro";
        String rtTableName = tableName + "_rt";

        if (tableType == HudiTableType.COPY_ON_WRITE) {
            assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50)) WITH (type='" + tableType.getName() + "')");
            assertTrue(getQueryRunner().tableExists(getSession(), tableName));
            assertTableColumnNames(tableName, "a", "b", "c");
            assertUpdate("DROP TABLE " + tableName);
            assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        }
        else if (tableType == HudiTableType.MERGE_ON_READ) {
            assertUpdate("CREATE TABLE " + roTableName + " (a bigint, b double, c varchar(50)) WITH (type='" + tableType.getName() + "')");
            assertUpdate("CREATE TABLE " + rtTableName + " (a bigint, b double, c varchar(50)) WITH (type='" + tableType.getName() + "')");
            assertTrue(getQueryRunner().tableExists(getSession(), roTableName));
            assertTableColumnNames(roTableName, "a", "b", "c");
            assertUpdate("DROP TABLE " + roTableName);
            assertFalse(getQueryRunner().tableExists(getSession(), roTableName));
            assertTrue(getQueryRunner().tableExists(getSession(), rtTableName));
            assertTableColumnNames(rtTableName, "a", "b", "c");
            assertUpdate("DROP TABLE " + rtTableName);
            assertFalse(getQueryRunner().tableExists(getSession(), rtTableName));
        }

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        if (tableType == HudiTableType.COPY_ON_WRITE) {
            assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        }
        else if (tableType == HudiTableType.MERGE_ON_READ) {
            assertFalse(getQueryRunner().tableExists(getSession(), roTableName));
            assertFalse(getQueryRunner().tableExists(getSession(), rtTableName));
        }
    }

    protected static String columnsToHide()
    {
        List<String> columns = new ArrayList<>(HOODIE_META_COLUMNS.size() + 1);
        columns.addAll(HOODIE_META_COLUMNS);
        columns.add(TpchHudiTablesInitializer.FIELD_UUID);
        return String.join(",", columns);
    }
}
