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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveS3MinioQueries
        extends AbstractTestQueryFramework
{
    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();

        return HiveQueryRunner.builder()
                .setMetastore(queryRunner -> {
                    File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
                    return new FileHiveMetastore(
                            new NodeVersion("testversion"),
                            HDFS_ENVIRONMENT,
                            new HiveMetastoreConfig().isHideDeltaLakeTables(),
                            new FileHiveMetastoreConfig()
                                    .setCatalogDirectory(baseDir.toURI().toString())
                                    .setDisableLocationChecks(true) // matches Glue behavior
                                    .setMetastoreUser("test"));
                })
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", minio.getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        minio = null; // closed by closeAfterClass
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testTableLocationTopOfTheBucket(boolean locationWithTrailingSlash)
    {
        String bucketName = "test-bucket-" + randomNameSuffix();
        minio.createBucket(bucketName);
        minio.writeFile("We are\nawesome at\nmultiple slashes.".getBytes(UTF_8), bucketName, "a_file");

        String location = "s3://%s%s".formatted(bucketName, locationWithTrailingSlash ? "/" : "");
        String tableName = "test_table_top_of_bucket_%s_%s".formatted(locationWithTrailingSlash, randomNameSuffix());
        String create = "CREATE TABLE %s (a varchar) WITH (format='TEXTFILE', external_location='%s')".formatted(tableName, location);
        if (!locationWithTrailingSlash) {
            assertQueryFails(create, "External location is not a valid file system URI: " + location);
            return;
        }
        assertUpdate(create);

        // Verify location was not normalized along the way. Glue would not do that.
        assertThat(getDeclaredTableLocation(tableName))
                .isEqualTo(location);

        assertThat(query("TABLE " + tableName))
                .matches("VALUES VARCHAR 'We are', 'awesome at', 'multiple slashes.'");

        assertUpdate("INSERT INTO " + tableName + " VALUES 'Aren''t we?'", 1);

        assertThat(query("TABLE " + tableName))
                .matches("VALUES VARCHAR 'We are', 'awesome at', 'multiple slashes.', 'Aren''t we?'");

        assertUpdate("DROP TABLE " + tableName);
    }

    private String getDeclaredTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*external_location = '(.*?)'.*", Pattern.DOTALL);
        Object result = computeScalar("SHOW CREATE TABLE " + tableName);
        Matcher matcher = locationPattern.matcher((String) result);
        if (matcher.find()) {
            String location = matcher.group(1);
            verify(!matcher.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in: " + result);
    }
}
