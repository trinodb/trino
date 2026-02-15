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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see iceberg.invalid_compression_codec
 */
public class TestIcebergInvalidCompressionCodecs
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "test-bucket";
    private static final String SCHEMA = "my_schema";
    private static final String AVRO_TABLE_NAME = "none_avro";
    private static final String PARQUET_TABLE_NAME = "none_parquet";
    private static final String ORC_TABLE_NAME = "orc_gzip";

    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET_NAME);

        QueryRunner queryRunner = IcebergQueryRunner.builder(SCHEMA)
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(SCHEMA)
                        .build())
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.aws-access-key", MINIO_ROOT_USER)
                                .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", minio.getMinioAddress())
                                .put("s3.path-style-access", "true")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .buildOrThrow())
                .build();

        minio.copyResources("iceberg/invalid_compression_codec/%s".formatted(AVRO_TABLE_NAME), BUCKET_NAME, AVRO_TABLE_NAME);
        minio.copyResources("iceberg/invalid_compression_codec/%s".formatted(PARQUET_TABLE_NAME), BUCKET_NAME, PARQUET_TABLE_NAME);
        minio.copyResources("iceberg/invalid_compression_codec/%s".formatted(ORC_TABLE_NAME), BUCKET_NAME, ORC_TABLE_NAME);
        return queryRunner;
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minio = null; // closed by closeAfterClass
    }

    @Test
    void testAvroNoneReadWrite()
    {
        try {
            assertUpdate(format(
                    "CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')",
                    AVRO_TABLE_NAME,
                    BUCKET_NAME,
                    AVRO_TABLE_NAME));
            assertThat(computeActual("SELECT * FROM \"%s\"".formatted(AVRO_TABLE_NAME)).getOnlyValue())
                    .isEqualTo("Hello World!");
            assertUpdate("INSERT INTO \"%s\" VALUES ('Goodbye World!')".formatted(AVRO_TABLE_NAME), 1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS \"%s\"".formatted(AVRO_TABLE_NAME));
        }
    }

    @Test
    void testParquetNoneReadWrite()
    {
        try {
            assertUpdate(format(
                    "CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')",
                    PARQUET_TABLE_NAME,
                    BUCKET_NAME,
                    PARQUET_TABLE_NAME));
            assertThat(computeActual("SELECT * FROM \"%s\"".formatted(PARQUET_TABLE_NAME)).getOnlyValue())
                    .isEqualTo("Hello World!");
            assertUpdate("INSERT INTO \"%s\" VALUES ('Goodbye World!')".formatted(PARQUET_TABLE_NAME), 1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS \"%s\"".formatted(PARQUET_TABLE_NAME));
        }
    }

    @Test
    void testOrcGzipReadWrite()
    {
        try {
            assertUpdate(format(
                    "CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')",
                    ORC_TABLE_NAME,
                    BUCKET_NAME,
                    ORC_TABLE_NAME));
            assertThat(computeActual("SELECT * FROM \"%s\"".formatted(ORC_TABLE_NAME)).getOnlyValue())
                    .isEqualTo("Hello World!");
            assertUpdate("INSERT INTO \"%s\" VALUES ('Goodbye World!')".formatted(ORC_TABLE_NAME), 1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS \"%s\"".formatted(ORC_TABLE_NAME));
        }
    }
}
