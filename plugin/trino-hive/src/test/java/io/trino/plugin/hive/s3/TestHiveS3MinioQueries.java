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
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveS3MinioQueries
        extends AbstractTestQueryFramework
{
    private Hive3MinioDataLake hiveMinioDataLake;
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-minio-queries-" + randomNameSuffix();
        this.hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName));
        this.hiveMinioDataLake.start();

        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testTableLocationTopOfTheBucket()
    {
        String bucketName = "test-bucket-" + randomNameSuffix();
        hiveMinioDataLake.getMinio().createBucket(bucketName);
        hiveMinioDataLake.getMinio().writeFile("We are\nawesome at\nmultiple slashes.".getBytes(UTF_8), bucketName, "a_file");

        // without trailing slash
        assertQueryFails(
                """
                CREATE TABLE %s (a varchar) WITH (
                    format='TEXTFILE',
                    external_location='%s'
                )
                """.formatted("test_table_top_of_bucket_" + randomNameSuffix(), "s3://" + bucketName),
                "External location is not a valid file system URI: s3://" + bucketName);

        // with trailing slash
        String location = "s3://%s/".formatted(bucketName);
        String tableName = "test_table_top_of_bucket_%s".formatted(randomNameSuffix());
        String create = "CREATE TABLE %s (a varchar) WITH (format='TEXTFILE', external_location='%s')".formatted(tableName, location);

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

    @Test
    public void testPathContainsSpecialCharacter()
    {
        String tableName = "test_path_special_character" + randomNameSuffix();
        String location = "s3://%s/%s/".formatted(bucketName, tableName);
        assertUpdate(format(
                "CREATE TABLE %s (id bigint, part varchar) WITH (partitioned_by = ARRAY['part'], external_location='%s')",
                tableName,
                location));

        String values = "(1, 'with-hyphen')," +
                "(2, 'with.dot')," +
                "(3, 'with:colon')," +
                "(4, 'with/slash')," +
                "(5, 'with\\\\backslashes')," +
                "(6, 'with\\backslash')," +
                "(7, 'with=equal')," +
                "(8, 'with?question')," +
                "(9, 'with!exclamation')," +
                "(10, 'with%percent')," +
                "(11, 'with%%percents')," +
                "(12, 'with space')";
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, 12);
        assertQuery("SELECT * FROM " + tableName, "VALUES " + values);
        assertUpdate("DROP TABLE " + tableName);
    }
}
