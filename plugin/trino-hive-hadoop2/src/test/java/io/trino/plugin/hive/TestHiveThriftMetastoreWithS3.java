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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TestHiveThriftMetastoreWithS3
        extends AbstractTestQueryFramework
{
    private final String awsS3endpoint;
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String writableBucket;
    private final String schemaName;
    private final Path hadoopCoreSiteXmlTempFile;

    @Parameters({
            "hive.hadoop2.s3.endpoint",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket",
    })
    public TestHiveThriftMetastoreWithS3(
            String awsS3endpoint,
            String awsAccessKey,
            String awsSecretKey,
            String writableBucket)
            throws IOException
    {
        this.awsS3endpoint = requireNonNull(awsS3endpoint, "awsS3endpoint is null");
        this.awsAccessKey = requireNonNull(awsAccessKey, "awsAccessKey is null");
        this.awsSecretKey = requireNonNull(awsSecretKey, "awsSecretKey is null");
        this.writableBucket = requireNonNull(writableBucket, "writableBucket is null");
        this.schemaName = "test_thrift_s3_" + randomTableSuffix();

        String coreSiteXmlContent = Resources.toString(Resources.getResource("s3/hive-core-site.template.xml"), UTF_8)
                .replace("%S3_BUCKET_ENDPOINT%", awsS3endpoint)
                .replace("%AWS_ACCESS_KEY_ID%", awsAccessKey)
                .replace("%AWS_SECRET_ACCESS_KEY%", awsSecretKey);

        hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--")));
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, coreSiteXmlContent);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveHadoop hiveHadoop = HiveHadoop.builder()
                .withFilesToMount(ImmutableMap.of("/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString()))
                .build();
        hiveHadoop.start();

        return S3HiveQueryRunner.builder()
                .setHiveMetastoreEndpoint(hiveHadoop.getHiveMetastoreEndpoint())
                .setS3Endpoint(awsS3endpoint)
                .setS3AccessKey(awsAccessKey)
                .setS3SecretKey(awsSecretKey)
                .setBucketName(writableBucket)
                .setPopulateTpchData(false)
                .setThriftMetastoreConfig(new ThriftMetastoreConfig().setDeleteFilesOnDrop(true))
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        String schemaLocation = "s3a://%s/%s".formatted(writableBucket, schemaName);
        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
    }

    @Test
    public void testRecreateTable()
    {
        String tableName = "%s.test_recreate_table_%s".formatted(schemaName, randomTableSuffix());
        assertUpdate("CREATE TABLE " + tableName + "(col int)");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
            assertUpdate("DROP TABLE " + tableName);

            // Recreating a table throws "Target directory for table 'xxx' already exists" in real S3 (not MinIO)
            // when 'hive.metastore.thrift.delete-files-on-drop' config property is false
            assertUpdate("CREATE TABLE " + tableName + "(col int)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
