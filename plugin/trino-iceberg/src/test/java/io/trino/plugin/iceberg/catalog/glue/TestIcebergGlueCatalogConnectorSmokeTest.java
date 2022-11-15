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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestView;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * TestIcebergGlueCatalogConnectorSmokeTest currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestIcebergGlueCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private final String schemaName;
    private final AWSGlueAsync glueClient;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Parameters("s3.bucket")
    public TestIcebergGlueCatalogConnectorSmokeTest(String bucketName)
    {
        super(FileFormat.PARQUET);
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.schemaName = "test_iceberg_smoke_" + randomNameSuffix();
        glueClient = AWSGlueAsyncClientBuilder.defaultClient();

        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of());
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        this.fileSystemFactory = new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaPath(),
                                "iceberg.register-table-procedure.enabled", "true",
                                "iceberg.writer-sort-buffer-size", "1MB"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder()
                                        .addAll(REQUIRED_TPCH_TABLES)
                                        .add(LINE_ITEM)
                                        .build())
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        computeActual("SHOW TABLES").getMaterializedRows()
                .forEach(table -> getQueryRunner().execute("DROP TABLE " + table.getField(0)));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);

        // DROP TABLES should clean up any files, but clear the directory manually to be safe
        deleteDirectory(schemaPath());
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(format("" +
                                "\\QCREATE TABLE iceberg.%1$s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   format_version = 2,\n" +
                                "   location = '%2$s/%1$s.db/region-\\E.*\\Q'\n" +
                                ")\\E",
                        schemaName,
                        schemaPath()));
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Test
    public void testCommentView()
    {
        // TODO: Consider moving to BaseConnectorSmokeTest
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("new comment");

            // comment updated
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(view.getName())).isEmpty();

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'a comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("a comment");
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(view.getName())).isNull();
        }
    }

    @Test
    public void testCommentViewColumn()
    {
        // TODO: Consider moving to BaseConnectorSmokeTest
        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment updated
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);
        }
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        glueClient.deleteTable(deleteTableRequest);
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        assertThatThrownBy(() -> glueClient.getTable(getTableRequest))
                .as("Table in metastore should not exist")
                .hasMessageMatching(".*Table (.*) not found.*");
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        return glueClient.getTable(getTableRequest)
                .getTable()
                .getParameters().get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(location);
        List<DeleteObjectsRequest.KeyVersion> keysToDelete = getPaginatedResults(
                s3::listObjectsV2,
                listObjectsRequest,
                ListObjectsV2Request::setContinuationToken,
                ListObjectsV2Result::getNextContinuationToken,
                new AwsApiCallStats())
                .map(ListObjectsV2Result::getObjectSummaries)
                .flatMap(objectSummaries -> objectSummaries.stream().map(S3ObjectSummary::getKey))
                .map(DeleteObjectsRequest.KeyVersion::new)
                .collect(toImmutableList());

        if (!keysToDelete.isEmpty()) {
            s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keysToDelete));
        }
        assertThat(s3.listObjects(bucketName, location).getObjectSummaries()).isEmpty();
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        String prefix = "s3://" + bucketName + "/";
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(location.substring(prefix.length()))
                .withMaxKeys(1);
        return !s3.listObjectsV2(request)
                .getObjectSummaries().isEmpty();
    }
}
