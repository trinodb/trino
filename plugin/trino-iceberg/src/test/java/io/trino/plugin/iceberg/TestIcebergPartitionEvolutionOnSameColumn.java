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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergPartitionEvolutionOnSameColumn
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "test-partition-evolution";

    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET_NAME);

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", minio.getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .buildOrThrow())
                .build();

        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + ICEBERG_CATALOG + ".tpch");
        return queryRunner;
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minio = null; // closed by closeAfterClass
    }

    @Test
    void testFilesPartitionEvolutionUsingTruncateOnSameColumn()
    {
        try (TestTable table = newTrinoTable("test_files_table", "(id int, part varchar) WITH (partitioning = ARRAY['truncate(part, 2)'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'India')", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, VARCHAR 'India')");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES ROW(CAST(ROW('In') AS ROW(part_trunc varchar)))");

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['truncate(part, 4)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 'Poland')", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['truncate(part, 2)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'Austria')", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['truncate(part, 4)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'France')", 1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, VARCHAR 'India'), (2, VARCHAR 'Poland'), (3, VARCHAR 'Austria'), (4, VARCHAR 'France')");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES " +
                            "ROW(CAST(ROW('In', null) AS ROW(part_trunc varchar, part_trunc_4 varchar))), " +
                            "ROW(CAST(ROW(null, 'Pola') AS ROW(part_trunc varchar, part_trunc_4 varchar))), " +
                            "ROW(CAST(ROW('Au', null) AS ROW(part_trunc varchar, part_trunc_4 varchar))), " +
                            "ROW(CAST(ROW(null, 'Fran') AS ROW(part_trunc varchar, part_trunc_4 varchar)))");
        }
    }

    @Test
    void testFilesPartitionEvolutionUsingBucketOnSameColumn()
    {
        try (TestTable table = newTrinoTable("test_files_table", "(id int, part varchar) WITH (partitioning = ARRAY['bucket(part, 2)'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'India')", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, VARCHAR 'India')");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES ROW(CAST(ROW(0) AS ROW(part_bucket int)))");

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['bucket(part, 4)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 'Poland')", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['bucket(part, 2)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'Austria')", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['bucket(part, 4)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'France')", 1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, VARCHAR 'India'), (2, VARCHAR 'Poland'), (3, VARCHAR 'Austria'), (4, VARCHAR 'France')");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES " +
                            "ROW(CAST(ROW(0, null) AS ROW(part_bucket int, part_bucket_4 int))), " +
                            "ROW(CAST(ROW(1, null) AS ROW(part_bucket int, part_bucket_4 int))), " +
                            "ROW(CAST(ROW(null, 1) AS ROW(part_bucket int, part_bucket_4 int))), " +
                            "ROW(CAST(ROW(null, 3) AS ROW(part_bucket int, part_bucket_4 int)))");
        }
    }

    /**
     * @see iceberg.conflict_truncate
     */
    @Test
    void testFilesPartitionEvolutionWithTruncateMetadataCorruption()
    {
        String tableName = "test_iceberg_partition_evolution_" + randomNameSuffix();

        minio.copyResources("iceberg/conflict_truncate", BUCKET_NAME, "conflict_truncate");
        assertUpdate(format(
                "CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')",
                tableName,
                format("s3://%s/conflict_truncate", BUCKET_NAME)));

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (VARCHAR 'abc'), (VARCHAR 'abcd')");

        // In the generated table, the latest metadata incorrectly reuses the same partition
        // field name for different truncate widths, each with its own field ID, resulting in an invalid schema
        assertThatThrownBy(() -> computeActual("SELECT partition FROM \"" + tableName + "$files\""))
                .hasMessage("Invalid schema: multiple fields for name partition.a_trunc: 1000 and 1001");

        // Fix partition evolution by setting the partitioning to use the same truncation level as the current configuration
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['truncate(a, 10)']");
        assertUpdate("INSERT INTO " + tableName + " VALUES 'abc_123456789'", 1);
        assertThat(query("SELECT partition FROM \"" + tableName + "$files\""))
                .matches("VALUES " +
                        "ROW(CAST(ROW('a', null) AS ROW(a_trunc varchar, a_trunc_10 varchar))), " +
                        "ROW(CAST(ROW(null, 'abcd') AS ROW(a_trunc varchar, a_trunc_10 varchar))), " +
                        "ROW(CAST(ROW(null, 'abc_123456') AS ROW(a_trunc varchar, a_trunc_10 varchar)))");

        // Use new truncation level to verify new partitioning works as expected
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['truncate(a, 5)']");
        assertUpdate("INSERT INTO " + tableName + " VALUES 'mnopqrst'", 1);
        assertThat(query("SELECT partition FROM \"" + tableName + "$files\""))
                .matches("VALUES " +
                        "ROW(CAST(ROW('a', null, null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, 'abcd', null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, 'abc_123456', null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, null, 'mnopq') AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar)))");

        // Using earlier truncation level (in the generated table) to use same partitioning column name again
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['truncate(a, 1)']");
        assertUpdate("INSERT INTO " + tableName + " VALUES 'abcdef'", 1);
        assertThat(query("SELECT partition FROM \"" + tableName + "$files\""))
                .matches("VALUES " +
                        "ROW(CAST(ROW('a', null, null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, 'abcd', null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, 'abc_123456', null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW(null, null, 'mnopq') AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar))), " +
                        "ROW(CAST(ROW('a', null, null) AS ROW(a_trunc varchar, a_trunc_10 varchar, a_trunc_5 varchar)))");

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (VARCHAR 'abc'), (VARCHAR 'abcd'), (VARCHAR 'abc_123456789'), (VARCHAR 'mnopqrst'), (VARCHAR 'abcdef')");

        assertUpdate("DROP TABLE " + tableName);
    }
}
