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
package io.trino.plugin.databricks;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.databricks.TestingDatabricksServer.TEST_SCHEMA;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDatabricksConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DatabricksQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return TestingDatabricksServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA,
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
                "test_unsupported_col",
                "(one bigint, two binary, three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("real")) {
            return Optional.empty();
        }
        if (typeName.equals("timestamp(6)")) {
            return Optional.empty();
        }
        if (typeName.equals("char(3)")) {
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return false;
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThatThrownBy(() -> { throw e; })
                .hasMessageContaining("NOT NULL");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL result in a non-nullable column";
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testInsertRowConcurrently()
    {
        abort("Databricks JDBC driver does not support concurrent inserts reliably");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThatThrownBy(() -> { throw e; })
                .hasMessageContaining("exceeds");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThatThrownBy(() -> { throw e; })
                .hasMessageContaining("exceeds");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(255);
    }
}
