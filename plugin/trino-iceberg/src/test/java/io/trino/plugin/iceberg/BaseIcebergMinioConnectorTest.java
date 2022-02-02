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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.SECRET_KEY;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergMinioConnectorTest
        extends BaseIcebergConnectorTest
{
    private static final List<String> TEST_REPARTITION_EXCLUSIONS = ImmutableList.of(
            TEST_REPARTITION_COMPLEX,
            TEST_REPARTITION_SAME_COLUMN_MULTIPLE_TIMES);

    private final String bucketName;
    private HiveMinioDataLake hiveMinioDataLake;

    public BaseIcebergMinioConnectorTest(IcebergFileFormat format)
    {
        super(format);
        this.bucketName = "test-iceberg-minio-integration-test-" + randomTableSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, ImmutableMap.of()));
        this.hiveMinioDataLake.start();
        return createQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .put("hive.s3.aws-access-key", ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", SECRET_KEY)
                        .put("hive.s3.endpoint", "http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                        .put("hive.s3.path-style-access", "true")
                        .put("hive.s3.streaming.part-size", "5MB")
                        .buildOrThrow());
    }

    @Override
    protected SchemaInitializer.Builder createSchemaInitializer(String schemaName)
    {
        return super.createSchemaInitializer(schemaName)
                .withSchemaProperties(ImmutableMap.of("location", "'s3://" + bucketName + "/" + schemaName + "'"));
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Override
    protected String createNewTableLocationBase(String schemaName)
    {
        return "s3://" + bucketName + "/" + schemaName;
    }

    @Override
    protected Long calculateFileSystemFileSize(String filePath)
    {
        return this.hiveMinioDataLake.getS3Client()
                .getObject(bucketName, filePath.replace("s3://" + bucketName + "/", ""))
                .getObjectMetadata()
                .getContentLength();
    }

    @Override
    protected BufferedInputStream createDataFileStreamForFileSystem(String filePath)
    {
        return new BufferedInputStream(this.hiveMinioDataLake.getS3Client()
                .getObject(bucketName, filePath.replace("s3://" + bucketName + "/", "")).getObjectContent());
    }

    @Override
    protected List<String> getAllDataFilesFromTableDirectory(String tableName)
    {
        String tableLocation = getSession().getSchema().orElseThrow() + "/" + tableName + "/data";
        return this.hiveMinioDataLake.getS3Client().listObjects(bucketName, tableLocation)
                .getObjectSummaries()
                .stream()
                .map(object -> "s3://" + bucketName + "/" + object.getKey())
                .filter(object -> !object.endsWith(".crc"))
                .collect(toImmutableList());
    }

    /**
     * DataProvider overload to exclude 2 test cases from generic tests:
     * - {@link BaseIcebergConnectorTest#testRepartitionDataOnCtas(Session, String, int)}
     * - {@link BaseIcebergConnectorTest#testRepartitionDataOnInsert(Session, String, int)}
     * <p>
     * 1. partitioning -> 'bucket(custkey, 4)', 'truncate(comment, 1)'
     * Test environment provides higher memory usage reaching over 1,5GB per test case.
     * This leads to test flakiness when both test classes run in parallel and may consume
     * over 3GB which is current hard limit for CI and may end up with OOM.
     * <p>
     * Limiting dataset to 300 rows in dedicated test methods for MinIO solves this issue:
     * - {@link BaseIcebergMinioConnectorTest#testRepartitionOnMinio()} ()}
     * <p>
     * 2. partitioning -> 'truncate(comment, 1)', 'orderstatus', 'bucket(comment, 2)'
     * Test environment causes HMS to consume more time during createTable operation.
     * This leads to read timed out and retry operation ended with TableAlreadyExists
     * (HMS managed to end operation on it's side). As such behaviour wasn't observed
     * in normal circumstances, test was adopted to avoid it.
     * <p>
     * Limiting dataset to 300 rows in dedicated test methods for MinIO solves this issue:
     * - {@link BaseIcebergMinioConnectorTest#testRepartitionOnMinio()} ()}
     */
    @Override
    @DataProvider
    public Object[][] repartitioningDataProvider()
    {
        Object[][] defaultTestsData = super.repartitioningDataProvider();
        List<Object[]> minioTestData = new ArrayList<>();
        for (Object[] testData : defaultTestsData) {
            if (!TEST_REPARTITION_EXCLUSIONS.contains(testData[1])) {
                minioTestData.add(testData);
            }
        }
        return minioTestData.toArray(new Object[minioTestData.size()][]);
    }

    @Test
    public void testRepartitionOnMinio()
    {
        String sourceQuery = "SELECT * FROM tpch.tiny.orders ORDER BY orderkey LIMIT 300";
        // complex; would exceed 100 open writers limit in IcebergPageSink without write repartitioning
        testRepartitionData(getSession(), sourceQuery, true, TEST_REPARTITION_COMPLEX, 84);
        testRepartitionData(getSession(), sourceQuery, false, TEST_REPARTITION_COMPLEX, 84);
        // with same column multiple times
        testRepartitionData(getSession(), sourceQuery, true, TEST_REPARTITION_SAME_COLUMN_MULTIPLE_TIMES, 97);
        testRepartitionData(getSession(), sourceQuery, false, TEST_REPARTITION_SAME_COLUMN_MULTIPLE_TIMES, 97);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE iceberg." + schemaName + ".region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + format + "',\n" +
                        "   location = 's3://" + bucketName + "/" + schemaName + "/region'\n" +
                        ")");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        // Overridden because error message is different from upper test method
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA " + schemaName).getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg." + schemaName + "\n" +
                        "AUTHORIZATION USER user\n" +
                        "WITH \\(\n" +
                        "\\s+location = 's3://" + bucketName + "/" + schemaName + "'\n" +
                        "\\)");
    }
}
