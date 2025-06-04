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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergParquetWithBloomFiltersMixedCase
        extends BaseTestParquetWithBloomFilters
{
    private static final String BUCKET_NAME = "test-bucket-mixed-case";

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

    @Override
    protected CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        minio.copyResources("iceberg/mixed_case_bloom_filter", BUCKET_NAME, "mixed_case_bloom_filter");
        String tableName = "test_iceberg_write_mixed_case_bloom_filter" + randomNameSuffix();
        assertUpdate(format(
                "CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')",
                tableName,
                format("s3://%s/mixed_case_bloom_filter", BUCKET_NAME)));

        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName("iceberg", new SchemaTableName("tpch", tableName));
        assertUpdate(format("INSERT INTO %s SELECT * FROM (VALUES %s) t(%s)", catalogSchemaTableName, Joiner.on(", ").join(testValues), columnName), testValues.size());

        checkTableProperties(tableName);

        return catalogSchemaTableName;
    }

    private void checkTableProperties(String tableName)
    {
        MaterializedResult actualProperties = computeActual("SELECT * FROM \"" + tableName + "$properties\"");
        assertThat(actualProperties).isNotNull();
        MaterializedResult expectedProperties = resultBuilder(getSession())
                .row("write.parquet.bloom-filter-enabled.column.dataColumn", "true")
                .build();
        assertContains(actualProperties, expectedProperties);

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("parquet_bloom_filter_columns");
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minio = null; // closed by closeAfterClass
    }
}
