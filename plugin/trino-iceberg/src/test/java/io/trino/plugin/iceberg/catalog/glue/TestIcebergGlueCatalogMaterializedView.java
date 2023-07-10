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
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestIcebergGlueCatalogMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private final String schemaName = "test_iceberg_materialized_view_" + randomNameSuffix();

    private File schemaDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.of())
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @Override
    protected String getSchemaDirectory()
    {
        return new File(schemaDirectory, schemaName + ".db").getPath();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        cleanUpSchema(schemaName);
        cleanUpSchema(storageSchemaName);
    }

    private static void cleanUpSchema(String schema)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Set<String> tableNames = getPaginatedResults(
                glueClient::getTables,
                new GetTablesRequest().withDatabaseName(schema),
                GetTablesRequest::setNextToken,
                GetTablesResult::getNextToken,
                new AwsApiCallStats())
                .map(GetTablesResult::getTableList)
                .flatMap(Collection::stream)
                .map(Table::getName)
                .collect(toImmutableSet());
        glueClient.batchDeleteTable(new BatchDeleteTableRequest()
                .withDatabaseName(schema)
                .withTablesToDelete(tableNames));
        glueClient.deleteDatabase(new DeleteDatabaseRequest()
                .withName(schema));
    }
}
