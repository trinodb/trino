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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveThriftMetastoreWithS3
        extends AbstractTestQueryFramework
{
    private final String s3endpoint;
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String writableBucket;
    private final String schemaName;
    private final Path hadoopCoreSiteXmlTempFile;
    private final AmazonS3 s3Client;

    @Parameters({
            "hive.hadoop2.s3.endpoint",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket",
    })
    public TestHiveThriftMetastoreWithS3(
            String s3endpoint,
            String awsAccessKey,
            String awsSecretKey,
            String writableBucket)
            throws IOException
    {
        this.s3endpoint = requireNonNull(s3endpoint, "s3endpoint is null");
        this.awsAccessKey = requireNonNull(awsAccessKey, "awsAccessKey is null");
        this.awsSecretKey = requireNonNull(awsSecretKey, "awsSecretKey is null");
        this.writableBucket = requireNonNull(writableBucket, "writableBucket is null");
        this.schemaName = "test_thrift_s3_" + randomNameSuffix();

        String coreSiteXmlContent = Resources.toString(Resources.getResource("s3/hive-core-site.template.xml"), UTF_8)
                .replace("%S3_BUCKET_ENDPOINT%", s3endpoint)
                .replace("%AWS_ACCESS_KEY_ID%", awsAccessKey)
                .replace("%AWS_SECRET_ACCESS_KEY%", awsSecretKey);

        hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--")));
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, coreSiteXmlContent);

        s3Client = AmazonS3Client.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3endpoint, null))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
                .build();
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
                .setS3Endpoint(s3endpoint)
                .setS3AccessKey(awsAccessKey)
                .setS3SecretKey(awsSecretKey)
                .setBucketName(writableBucket)
                .setCreateTpchSchemas(false)
                .setThriftMetastoreConfig(new ThriftMetastoreConfig().setDeleteFilesOnDrop(true))
                .setHiveProperties(ImmutableMap.of("hive.allow-register-partition-procedure", "true"))
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
        String tableName = "test_recreate_table_" + randomNameSuffix();
        String schemaTableName = "%s.%s".formatted(schemaName, tableName);
        String tableLocation = "%s/%s".formatted(schemaName, tableName);

        // Creating a new table generates special empty file on S3 (not MinIO)
        assertUpdate("CREATE TABLE " + schemaTableName + "(col int)");
        try {
            assertUpdate("INSERT INTO " + schemaTableName + " VALUES (1)", 1);
            assertThat(getS3ObjectSummaries(tableLocation)).hasSize(2); // directory + file

            // DROP TABLE with Thrift metastore on S3 (not MinIO) leaves some files
            // when 'hive.metastore.thrift.delete-files-on-drop' config property is false.
            // Then, the subsequent CREATE TABLE throws "Target directory for table 'xxx' already exists"
            assertUpdate("DROP TABLE " + schemaTableName);
            assertThat(getS3ObjectSummaries(tableLocation)).hasSize(0);

            assertUpdate("CREATE TABLE " + schemaTableName + "(col int)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    public void testRecreatePartition()
    {
        String tableName = "test_recreate_partition_" + randomNameSuffix();
        String schemaTableName = "%s.%s".formatted(schemaName, tableName);
        String partitionLocation = "%s/%s/part=1".formatted(schemaName, tableName);

        assertUpdate("CREATE TABLE " + schemaTableName + "(col int, part int) WITH (partitioned_by = ARRAY['part'])");
        try {
            // Creating an empty partition generates special empty file on S3 (not MinIO)
            assertUpdate("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['1'])".formatted(schemaName, tableName));
            assertUpdate("INSERT INTO " + schemaTableName + " VALUES (1, 1)", 1);
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 1)");

            assertThat(getS3ObjectSummaries(partitionLocation)).hasSize(2); // directory + file

            // DELETE with Thrift metastore on S3 (not MinIO) leaves some files
            // when 'hive.metastore.thrift.delete-files-on-drop' config property is false.
            // Then, the subsequent SELECT doesn't return an empty row
            assertUpdate("DELETE FROM " + schemaTableName);
            assertThat(getS3ObjectSummaries(partitionLocation)).hasSize(0);

            assertUpdate("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['1'])".formatted(schemaName, tableName));
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);
        }
        finally {
            assertUpdate("DROP TABLE " + schemaTableName);
        }
    }

    @Test
    public void testUnregisterPartitionNotRemoveData()
    {
        // Verify unregister_partition procedure doesn't remove physical data even when 'hive.metastore.thrift.delete-files-on-drop' config property is true
        String tableName = "test_recreate_partition_" + randomNameSuffix();
        String schemaTableName = "%s.%s".formatted(schemaName, tableName);

        assertUpdate("CREATE TABLE " + schemaTableName + "(col int, part int) WITH (partitioned_by = ARRAY['part'])");
        try {
            assertUpdate("INSERT INTO " + schemaTableName + " VALUES (1, 1)", 1);
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 1)");

            assertUpdate("CALL system.unregister_partition('%s', '%s', ARRAY['part'], ARRAY['1'])".formatted(schemaName, tableName));
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.register_partition('%s', '%s', ARRAY['part'], ARRAY['1'])".formatted(schemaName, tableName));
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 1)");
        }
        finally {
            assertUpdate("DROP TABLE " + schemaTableName);
        }
    }

    private List<S3ObjectSummary> getS3ObjectSummaries(String prefix)
    {
        return s3Client.listObjectsV2(writableBucket, prefix).getObjectSummaries();
    }
}
