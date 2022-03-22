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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseDeltaLakeMinioConnectorTest
        extends BaseConnectorTest
{
    private static final String SCHEMA = "test_schema";

    protected String bucketName;
    protected String resourcePath;
    protected DockerizedMinioDataLake dockerizedMinioDataLake;

    public BaseDeltaLakeMinioConnectorTest(String bucketName, String resourcePath)
    {
        this.bucketName = requireNonNull(bucketName);
        this.resourcePath = requireNonNull(resourcePath);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(bucketName, Optional.empty()));
        QueryRunner queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.<String, String>builder()
                        .put("delta.enable-non-concurrent-writes", "true")
                        .buildOrThrow(),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
        TpchTable.getTables().forEach(table -> {
            String tableName = table.getTableName();
            dockerizedMinioDataLake.copyResources(resourcePath + tableName, SCHEMA + "/" + tableName);
            queryRunner.execute(format("CREATE TABLE %1$s.%2$s.%3$s (dummy int) WITH (location = 's3://%4$s/%2$s/%3$s')",
                    DELTA_CATALOG,
                    SCHEMA,
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_ROW_LEVEL_DELETE:
                return true;
            case SUPPORTS_UPDATE:
                return true;
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .getCause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time") || typeName.equals("timestamp") || typeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // TODO https://github.com/trinodb/trino/issues/11297: these should be cleanly rejected and filterColumnNameTestData() replaced with isColumnNameRejected()
        Set<String> unsupportedColumnNames = ImmutableSet.of(
                "atrailingspace ",
                " aleadingspace",
                "a,comma",
                "a;semicolon",
                "a space");
        if (unsupportedColumnNames.contains(columnName)) {
            return Optional.empty();
        }

        return Optional.of(columnName);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Delta Lake does not support columns with a default value");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/test_schema/orders',\n\\Q" +
                        "   partitioned_by = ARRAY[]\n" +
                        ")");
    }

    @Override
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .isEqualTo(format("CREATE SCHEMA %s.%s\n" +
                        "WITH (\n" +
                        "   location = 's3://%s/test_schema'\n" +
                        ")", getSession().getCatalog().orElseThrow(), schemaName, bucketName));
    }

    @Override
    public void testDropNonEmptySchema()
    {
        String schemaName = "test_drop_non_empty_schema_" + randomTableSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')");
        assertUpdate("CREATE TABLE " + schemaName + ".t(x int)");
        assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        assertUpdate("DROP TABLE " + schemaName + ".t");
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Override
    public void testCharVarcharComparison()
    {
        // Delta Lake doesn't have a char type
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasStackTraceContaining("Unsupported type: char(3)");
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }
}
