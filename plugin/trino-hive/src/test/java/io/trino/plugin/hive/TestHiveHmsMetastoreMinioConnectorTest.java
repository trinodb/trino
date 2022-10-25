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
package io.trino.plugin.hive;

import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.OptionalInt;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Similar to {@link TestHiveConnectorTest} but uses Hive metastore (HMS) and MinIO
 * (S3-compatible storage) instead of file metastore and local file system.
 */
public class TestHiveHmsMetastoreMinioConnectorTest
        extends BaseHiveConnectorTest
{
    private final String bucketName = "test-hive-metastore-minio-" + randomTableSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .setHiveProperties(Map.of(
                        // Reduce memory pressure when writing large files
                        "hive.s3.streaming.part-size", new HiveS3Config().getS3MultipartMinPartSize().toString()))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void verifyTestDataSetup()
    {
        assertThat(computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*') FROM nation"))
                .isEqualTo(format("s3a://%s/tpch/nation", bucketName));
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return format("CREATE SCHEMA %1$s WITH (location='s3a://%2$s/%1$s')", schemaName, bucketName);
    }

    @Override
    public void testAddColumnConcurrently()
    {
        // TODO (https://github.com/trinodb/trino/issues/14745) adding columns may overwrite concurrent addition of columns (or some other operations)
        //  because adding columns currently consist of "read table, derive new table object, persist" with no locking (pessimistic nor optimistic)
        throw new SkipException("The test may or may not fail, because there is a concurrency issue");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }
}
