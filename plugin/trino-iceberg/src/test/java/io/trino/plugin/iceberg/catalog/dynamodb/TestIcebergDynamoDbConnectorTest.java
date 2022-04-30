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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergDynamoDbConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergDynamoDbConnectorTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingIcebergDynamoDbServer dynamoServer = closeAfterClass(new TestingIcebergDynamoDbServer());
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", ORC.name())
                                .put("iceberg.catalog.type", "dynamo_db")
                                .put("iceberg.dynamodb.catalog-name", "test")
                                .put("iceberg.dynamodb.connection-url", dynamoServer.getEndpointUrl())
                                .put("iceberg.dynamodb.aws.region", TestingIcebergDynamoDbServer.REGION)
                                .put("iceberg.dynamodb.aws.access-key", TestingIcebergDynamoDbServer.ACCESS_KEY)
                                .put("iceberg.dynamodb.aws.secret-key", TestingIcebergDynamoDbServer.SECRET_KEY)
                                .buildOrThrow())
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .isEqualTo("CREATE SCHEMA iceberg.tpch");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessage("renameNamespace is not supported for Iceberg DynamoDB catalogs");
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
    public void testShowCreateView()
    {
        assertThatThrownBy(super::testShowCreateView)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView2()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView2)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithView()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithView)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test(dataProvider = "testViewMetadataDataProvider")
    @Override
    public void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        assertThatThrownBy(() -> super.testViewMetadata(securityClauseInCreate, securityClauseInShowCreate))
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testViewCaseSensitivity()
    {
        assertThatThrownBy(super::testViewCaseSensitivity)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testViewAndMaterializedViewTogether()
    {
        assertThatThrownBy(super::testViewAndMaterializedViewTogether)
                .hasMessage("createView is not supported for Iceberg DynamoDB catalogs");
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
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        assertThatThrownBy(super::testReadMetadataWithRelationsConcurrentModifications)
                .hasMessage("java.lang.RuntimeException: createMaterializedView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    @Override
    public void testMaterializedViewColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testMaterializedViewColumnName(columnName))
                .hasMessage("createMaterializedView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg DynamoDB catalogs");
    }

    @Test
    @Override
    public void testRenameMaterializedView()
    {
        assertThatThrownBy(super::testRenameMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg DynamoDB catalogs");
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary")) &&
                !(typeName.equalsIgnoreCase("uuid"));
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "10")
                .build();
    }
}
