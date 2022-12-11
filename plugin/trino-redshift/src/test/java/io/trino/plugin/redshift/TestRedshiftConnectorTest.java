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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRedshiftConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRedshiftQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                // NOTE this can cause tests to time-out if larger tables like
                //  lineitem and orders need to be re-created.
                TpchTable.getTables());
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch") // options here are grouped per-feature
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                format("%s.test_table_with_default_columns", TEST_SCHEMA),
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if ("date".equals(typeName)) {
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
        }
        if ("tinyint".equals(typeName) || typeName.startsWith("time") || "varbinary".equals(typeName)) {
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    /**
     * Overridden due to Redshift not supporting non-ASCII characters in CHAR.
     */
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("Value too long for character type");
        // NOTE we add a copy of the above using VARCHAR which supports non-ASCII characters
        assertCreateTableAsSelect(
                "SELECT CAST('\u2603' AS VARCHAR) unicode",
                "SELECT 1");
    }

    @Override
    @Test
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s).*Cannot insert a NULL value into column %s.*", columnName);
    }

    @Override
    public void testCreateSchemaWithLongName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testRenameSchemaToLongName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testRenameTableToLongTableName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        throw new SkipException("Long name checks not implemented");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return RedshiftQueryRunner::executeInRedshift;
    }

    @Test
    @Override
    public void testAddNotNullColumnToNonEmptyTable()
    {
        throw new SkipException("Redshift ALTER TABLE ADD COLUMN defined as NOT NULL must have a non-null default expression");
    }
}
