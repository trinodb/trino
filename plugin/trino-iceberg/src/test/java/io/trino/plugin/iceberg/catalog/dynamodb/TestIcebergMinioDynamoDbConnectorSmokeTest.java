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
package io.trino.plugin.iceberg.catalog.dynamodb;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.iceberg.BaseIcebergMinioConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.hive.containers.HiveMinioDataLake.ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.SECRET_KEY;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergMinioDynamoDbConnectorSmokeTest
        extends BaseIcebergMinioConnectorSmokeTest
{
    public TestIcebergMinioDynamoDbConnectorSmokeTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, ImmutableMap.of()));
        this.hiveMinioDataLake.start();

        TestingIcebergDynamoDbServer dynamoServer = closeAfterClass(new TestingIcebergDynamoDbServer());
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "dynamo_db")
                                .put("iceberg.dynamodb.catalog-name", "test")
                                .put("iceberg.dynamodb.connection-url", dynamoServer.getEndpointUrl())
                                .put("hive.s3.aws-access-key", ACCESS_KEY)
                                .put("hive.s3.aws-secret-key", SECRET_KEY)
                                .put("hive.s3.endpoint", "http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                                .put("hive.s3.path-style-access", "true")
                                .put("hive.s3.streaming.part-size", "5MB")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/" + schemaName + "'"))
                                .build())
                .build();
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                "renameNamespace is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        throw new SkipException("TODO: Fix test failure");
    }
}
