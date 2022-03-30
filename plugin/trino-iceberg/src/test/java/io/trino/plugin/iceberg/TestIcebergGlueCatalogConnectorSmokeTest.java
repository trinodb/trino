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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
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

    @Parameters("s3.bucket")
    public TestIcebergGlueCatalogConnectorSmokeTest(String bucketName)
    {
        super(FileFormat.PARQUET);
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.schemaName = "iceberg_smoke_test_" + randomTableSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaPath()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
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
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(schemaPath());
        List<DeleteObjectsRequest.KeyVersion> keysToDelete = getPaginatedResults(
                s3::listObjectsV2,
                listObjectsRequest,
                ListObjectsV2Request::setContinuationToken,
                ListObjectsV2Result::getNextContinuationToken,
                new GlueMetastoreApiStats())
                .map(ListObjectsV2Result::getObjectSummaries)
                .flatMap(objectSummaries -> objectSummaries.stream().map(S3ObjectSummary::getKey))
                .map(DeleteObjectsRequest.KeyVersion::new)
                .collect(toImmutableList());

        if (!keysToDelete.isEmpty()) {
            s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keysToDelete));
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(format("" +
                                "CREATE TABLE iceberg.%1$s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   location = '%2$s/%1$s.db/region'\n" +
                                ")",
                        schemaName,
                        schemaPath()));
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("createView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    private String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }
}
