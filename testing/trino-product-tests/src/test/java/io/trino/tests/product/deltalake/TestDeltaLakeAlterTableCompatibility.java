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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Delta Lake ALTER TABLE compatibility between Trino and Spark.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeAlterTableCompatibility
{
    @Test
    void testAddColumnWithCommentOnTrino(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_add_column_with_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT COMMENT 'new column comment'");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "new_col")).isEqualTo("new column comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "new_col")).isEqualTo("new column comment");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testRenameColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_rename_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("" +
                        "CREATE TABLE default.%s (col INT) " +
                        "USING DELTA LOCATION 's3://%s/%s' " +
                        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1)");
            assertThat(env.executeTrino("SELECT col FROM delta.default." + tableName))
                    .containsOnly(row(1));

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN col TO new_col");
            assertThat(env.executeTrino("SELECT new_col FROM delta.default." + tableName))
                    .containsOnly(row(1));

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (2)");
            assertThat(env.executeTrino("SELECT new_col FROM delta.default." + tableName))
                    .containsOnly(row(1), row(2));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testRenamePartitionedColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_rename_partitioned_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("" +
                        "CREATE TABLE default.%s (col INT, part STRING) " +
                        "USING DELTA LOCATION 's3://%s/%s' " +
                        "PARTITIONED BY (part) " +
                        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'part1')");
            assertThat(env.executeTrino("SELECT col, part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"));

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN part TO new_part");
            assertThat(env.executeTrino("SELECT col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"));

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (2, 'part2')");
            assertThat(env.executeTrino("SELECT col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"), row(2, "part2"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDropNotNullConstraint(DeltaLakeMinioEnvironment env)
    {
        testDropNotNullConstraint("id", env);
        testDropNotNullConstraint("name", env);
        testDropNotNullConstraint("none", env);
    }

    private void testDropNotNullConstraint(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_drop_not_null_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(data1 int NOT NULL, data2 int NOT NULL, part1 int NOT NULL, part2 int NOT NULL) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "PARTITIONED BY (part1, part2)" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + columnMappingMode + "')");
        try {
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ALTER COLUMN data1 DROP NOT NULL");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ALTER COLUMN part1 DROP NOT NULL");

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ALTER COLUMN data2 DROP NOT NULL");
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ALTER COLUMN part2 DROP NOT NULL");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (NULL, NULL, NULL, NULL)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, NULL, NULL)");

            List<Row> expected = ImmutableList.of(row(null, null, null, null), row(null, null, null, null));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testCommentOnTable(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_comment_table_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment'");
            assertThat(getTableCommentOnTrino(env, "default", tableName)).isEqualTo("test comment");
            assertThat(getTableCommentOnSpark(env, "default", tableName)).isEqualTo("test comment");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testCommentOnColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_comment_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test column comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "col")).isEqualTo("test column comment");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testTrinoPreservesReaderAndWriterVersions(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_trino_preserves_versions_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minReaderVersion'='1', 'delta.minWriterVersion'='1', 'delta.checkpointInterval' = 1)",
                tableName,
                env.getBucketName(),
                tableDirectory));
        try {
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET col = 2");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName);
            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " + "ON (t.col = s.col) WHEN MATCHED THEN UPDATE SET new_col = 3");

            List<?> minReaderVersion = getOnlyElement(env.executeSpark("SHOW TBLPROPERTIES " + tableName + "(delta.minReaderVersion)").rows());
            assertThat((String) minReaderVersion.get(1)).isEqualTo("1");
            List<?> minWriterVersion = getOnlyElement(env.executeSpark("SHOW TBLPROPERTIES " + tableName + "(delta.minWriterVersion)").rows());
            assertThat((String) minWriterVersion.get(1)).isEqualTo("1");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testTrinoPreservesTableFeature(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_trino_preserves_table_feature_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName + " (col int)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.checkpointInterval'=1, 'delta.feature.columnMapping'='supported')");
        try {
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET col = 2");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName);
            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " + "ON (t.col = s.col) WHEN MATCHED THEN UPDATE SET new_col = 3");

            assertThat(getTablePropertyOnSpark(env, "default", tableName, "delta.feature.columnMapping"))
                    .isEqualTo("supported");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTypeWideningInteger(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_type_widening_integer_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName + "" +
                "(a byte, b byte) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.enableTypeWidening'=true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (127, -128)");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128));

            // byte -> short
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE short");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128));
            env.executeSparkUpdate("INSERT INTO default." + tableName + " (a) VALUES 32767");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, null));

            // byte -> integer
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " CHANGE COLUMN b TYPE integer");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, null));
            env.executeSparkUpdate("UPDATE default." + tableName + " SET b = -32768 WHERE b IS NULL");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768));

            // short -> integer
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE integer");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768));
            env.executeSparkUpdate("INSERT INTO default." + tableName + " (a) VALUES 2147483647");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768), row(2147483647, null));

            // integer -> long
            // TODO: Add support for reading long type values after Delta column type evolution from integer to long
            // Unsupported type widening is tested in TestDeltaLakeBasic.testTypeWideningSkippingUnsupportedColumns
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE long");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    // Helper methods for getting comments and properties

    private String getColumnCommentOnTrino(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeTrino("SELECT comment FROM delta.information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    private String getColumnCommentOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeSpark(format("DESCRIBE %s.%s %s", schemaName, tableName, columnName))
                .rows().get(2).get(1);
    }

    private String getTableCommentOnTrino(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return (String) env.executeTrino("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'")
                .getOnlyValue();
    }

    private String getTableCommentOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return (String) env.executeSpark(format("DESCRIBE EXTENDED %s.%s", schemaName, tableName))
                .rows().stream()
                .filter(row -> row.get(0).equals("Comment"))
                .map(row -> row.get(1))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Comment not found for table " + schemaName + "." + tableName));
    }

    private String getTablePropertyOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String propertyName)
    {
        return (String) getOnlyElement(env.executeSpark("SHOW TBLPROPERTIES %s.%s(%s)".formatted(schemaName, tableName, propertyName)).rows()).get(1);
    }
}
