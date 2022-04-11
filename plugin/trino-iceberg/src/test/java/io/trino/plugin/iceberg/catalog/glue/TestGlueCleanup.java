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
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;

import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;

/**
 * Test class with methods to clean up existing Glue tables and schemas from test which were not finished properly.
 */
public class TestGlueCleanup
{
    // All tests creating Glue schemas should add this key/value pair to the schema properties.
    public static final String GLUE_SCHEMA_PROPERTY_KEY = "ci_iceberg_integration_test";
    public static final String GLUE_SCHEMA_PROPERTY_VALUE = "true";

    private static final Logger LOG = Logger.get(TestGlueCleanup.class);

    public static void setSchemaFlag(String schemaName)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Database database = glueClient.getDatabase(new GetDatabaseRequest().withName(schemaName)).getDatabase();
        ImmutableMap.Builder<String, String> schemaParameters = ImmutableMap.builder();
        if (database.getParameters() != null) {
            schemaParameters.putAll(database.getParameters());
        }
        schemaParameters.put(GLUE_SCHEMA_PROPERTY_KEY, GLUE_SCHEMA_PROPERTY_VALUE);

        DatabaseInput databaseInput = new DatabaseInput()
                .withName(schemaName)
                .withDescription(database.getDescription())
                .withLocationUri(database.getLocationUri())
                .withParameters(schemaParameters.buildOrThrow())
                .withCreateTableDefaultPermissions(database.getCreateTableDefaultPermissions())
                .withTargetDatabase(database.getTargetDatabase());
        glueClient.updateDatabase(new UpdateDatabaseRequest()
                .withName(schemaName)
                .withDatabaseInput(databaseInput));
    }

    @Test
    public void cleanupOrphanedSchemas()
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        long createdAtCutoff = System.currentTimeMillis() - Duration.ofDays(1).toMillis();
        getPaginatedResults(
                glueClient::getDatabases,
                new GetDatabasesRequest(),
                GetDatabasesRequest::setNextToken,
                GetDatabasesResult::getNextToken,
                new GlueMetastoreApiStats())
                .map(GetDatabasesResult::getDatabaseList)
                .flatMap(Collection::stream)
                .filter(database -> database.getParameters() != null && GLUE_SCHEMA_PROPERTY_VALUE.equals(database.getParameters().get(GLUE_SCHEMA_PROPERTY_KEY)))
                .filter(database -> database.getCreateTime().before(new Date(createdAtCutoff)))
                .forEach(database -> {
                    LOG.warn("Deleting old Glue database " + database);
                    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(database.getName()));
                });
    }
}
