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
package io.trino.plugin.storage;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

final class TestStorageConnector
        extends AbstractTestQueryFramework
{
    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket("test-bucket");
        return StorageQueryRunner.builder()
                .addConnectorProperty("fs.hadoop.enabled", "false")
                .addConnectorProperty("fs.native-s3.enabled", "true")
                .addConnectorProperty("s3.endpoint", minio.getMinioAddress())
                .addConnectorProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addConnectorProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addConnectorProperty("s3.region", MINIO_REGION)
                .addConnectorProperty("s3.path-style-access", "true")
                .build();
    }

    @Test
    void testListFiles()
    {
        minio.writeFile("test".getBytes(UTF_8), "test-bucket", "/list_files/test.txt");
        assertQuery(
                "SELECT name " +
                        "FROM TABLE(storage.system.list_files('s3://test-bucket/list_files'))",
                "VALUES 's3://test-bucket/list_files/test.txt'");
    }

    @Test
    void testReadCsv()
    {
        minio.copyResources("example-data/numbers.csv", "test-bucket", "numbers.csv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/numbers.csv'))",
                "VALUES ('eleven', '11'), ('twelve', '12')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/numbers.csv', ','))",
                "VALUES ('eleven', '11'), ('twelve', '12')");

        minio.copyResources("example-data/quoted_fields_with_separator.csv", "test-bucket", "quoted_fields_with_separator.csv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/quoted_fields_with_separator.csv'))",
                "VALUES " +
                        "('test','2','3','4')," +
                        "('test,test,test,test','3','3','5')," +
                        "(' even weirder, but still valid, value with extra whitespaces that remain due to quoting /  ','1','2','3')," +
                        "('extra whitespaces that should get trimmed due to no quoting','1','2','3')");

        minio.copyResources("example-data/quoted_fields_with_newlines.csv", "test-bucket", "quoted_fields_with_newlines.csv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/quoted_fields_with_newlines.csv'))",
                "VALUES ('test','2','3','4')," +
                        "('test,test,test,test','3','3','5')," +
                        "(' even weirder, but still valid, value with linebreaks and extra\nwhitespaces that should remain due to quoting   ','1','2','3')," +
                        "('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
    }

    @Test
    void testReadTsv()
    {
        minio.copyResources("example-data/numbers.tsv", "test-bucket", "numbers.tsv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/numbers.tsv', '\t'))",
                "VALUES ('1', 'alice'), ('2', 'bob')");

        minio.copyResources("example-data/quoted_fields_with_separator.tsv", "test-bucket", "quoted_fields_with_separator.tsv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/quoted_fields_with_separator.tsv', '\t'))",
                "VALUES " +
                        "('test','2','3','4')," +
                        "('test\ttest\ttest\ttest','3','3','5')," +
                        "(' even weirder\t but still valid\t value with extra whitespaces that remain due to quoting /  ','1','2','3')," +
                        "('extra whitespaces that should get trimmed due to no quoting','1','2','3')");

        minio.copyResources("example-data/quoted_fields_with_newlines.tsv", "test-bucket", "quoted_fields_with_newlines.tsv");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_csv('s3://test-bucket/quoted_fields_with_newlines.tsv', '\t'))",
                "VALUES " +
                        "('test','2','3','4'),('test\ttest\ttest\ttest','3','3','5')," +
                        "(' even weirder, but still valid, value with linebreaks and extra\n" +
                        " whitespaces that should remain due to quoting   ','1','2','3')," +
                        "('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
    }
}
