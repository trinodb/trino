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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.hudi.common.model.HoodieTableType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.hudi.HudiQueryRunner.createHudiQueryRunner;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHudiConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("hudi.columns-to-hide", columnsToHide()),
                new TpchHudiTablesInitializer(getHoodieTableType(), REQUIRED_TPCH_TABLES));
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            // Optimizer
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            // DDL and DML on schemas and tables
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            // DDL and DML on columns
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            // Writing capabilities
            case SUPPORTS_DELETE:
            case SUPPORTS_INSERT:
            case SUPPORTS_MULTI_STATEMENT_WRITES:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
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

    protected abstract HoodieTableType getHoodieTableType();

    static String columnsToHide()
    {
        List<String> columns = new ArrayList<>(HOODIE_META_COLUMNS.size() + 1);
        columns.addAll(HOODIE_META_COLUMNS);
        columns.add(TpchHudiTablesInitializer.FIELD_UUID);
        return String.join(",", columns);
    }
}
