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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;

// Single-threaded because H2 DDL operations can sometimes take a global lock, leading to apparent deadlocks
// like in https://github.com/trinodb/trino/issues/7209.
@Test(singleThreaded = true)
public class TestJdbcConnectorTest
        extends BaseJdbcConnectorTest
{
    private Map<String, String> properties;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        properties = ImmutableMap.<String, String>builder()
                .putAll(TestingH2JdbcModule.createProperties())
                .put("allow-drop-table", "true")
                .build();
        return createH2QueryRunner(REQUIRED_TPCH_TABLES, properties);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    @Test(dataProvider = "largeInValuesCount")
    public void testLargeIn(int valuesCount)
    {
        throw new SkipException("This test should pass with H2, but takes too long (currently over a mninute) and is not that important");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two geometry, three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeBaseName = dataMappingTestSetup.getTrinoTypeName().replaceAll("\\([^()]*\\)", "");
        switch (typeBaseName) {
            case "boolean":
            case "decimal":
            case "char":
            case "varbinary":
            case "time":
            case "timestamp":
            case "timestamp with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeBaseName = dataMappingTestSetup.getTrinoTypeName().replaceAll("\\([^()]*\\)", "");
        switch (typeBaseName) {
            case "char":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected JdbcSqlExecutor onRemoteDatabase()
    {
        return new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
    }
}
