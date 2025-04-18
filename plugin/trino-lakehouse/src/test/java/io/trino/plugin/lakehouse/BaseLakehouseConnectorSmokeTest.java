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
package io.trino.plugin.lakehouse;

import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseLakehouseConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    protected final TableType tableType;

    protected BaseLakehouseConnectorSmokeTest(TableType tableType)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinio = closeAfterClass(new Hive3MinioDataLake(bucketName));
        hiveMinio.start();

        return LakehouseQueryRunner.builder()
                .addLakehouseProperty("lakehouse.table-type", tableType.name())
                .addLakehouseProperty("hive.metastore", "thrift")
                .addLakehouseProperty("hive.metastore.uri", hiveMinio.getHiveMetastoreEndpoint().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .addLakehouseProperty("fs.native-s3.enabled", "true")
                .addLakehouseProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addLakehouseProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addLakehouseProperty("s3.region", MINIO_REGION)
                .addLakehouseProperty("s3.endpoint", hiveMinio.getMinio().getMinioAddress())
                .addLakehouseProperty("s3.path-style-access", "true")
                .addLakehouseProperty("s3.streaming.part-size", "5MB")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        computeActual("CREATE SCHEMA lakehouse.tpch WITH (location='s3://%s/tpch')".formatted(bucketName));
        copyTpchTables(getQueryRunner(), "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails(
                "ALTER SCHEMA tpch RENAME TO tpch_" + randomNameSuffix(),
                "Hive metastore does not support renaming schemas");
    }

    @Test
    public void testCreateHiveTable()
    {
        computeActual(
                """
                CREATE TABLE create_hive
                WITH (
                    format = 'RCBINARY',
                    type = 'HIVE'
                )
                AS SELECT * FROM tpch.tiny.region
                """);

        assertThat((String) computeScalar("SHOW CREATE TABLE create_hive")).isEqualTo(
                """
                CREATE TABLE lakehouse.tpch.create_hive (
                   regionkey bigint,
                   name varchar(25),
                   comment varchar(152)
                )
                WITH (
                   format = 'RCBINARY',
                   type = 'HIVE'
                )""");

        assertUpdate("DROP TABLE create_hive");
    }

    @Test
    public void testCreateIcebergTable()
    {
        computeActual(
                """
                CREATE TABLE create_iceberg
                WITH (
                    format = 'ORC',
                    type = 'ICEBERG'
                )
                AS SELECT * FROM tpch.tiny.region
                """);

        assertThat((String) computeScalar("SHOW CREATE TABLE create_iceberg")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.create_iceberg (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'ORC',
                   format_version = 2,
                   location = \\E's3://test-bucket-.*/tpch/create_iceberg-.*'\\Q,
                   max_commit_retry = 4,
                   type = 'ICEBERG'
                )\\E""");

        assertUpdate("DROP TABLE create_iceberg");
    }

    @Test
    public void testCreateDeltaTable()
    {
        computeActual(
                """
                CREATE TABLE create_delta
                WITH (
                    type = 'DELTA'
                )
                AS SELECT * FROM tpch.tiny.region
                """);

        assertThat((String) computeScalar("SHOW CREATE TABLE create_delta")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.create_delta (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   location = \\E's3://test-bucket-.*/tpch/create_delta-.*'\\Q,
                   type = 'DELTA'
                )\\E""");

        assertUpdate("DROP TABLE create_delta");
    }
}
