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
package io.trino.tests.product.deltalake;

import io.airlift.log.Logger;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;

import java.util.List;

import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestDeltaLakeDatabricksCleanUpGlueMetastore
        extends ProductTest
{
    private static final Logger log = Logger.get(TestDeltaLakeDatabricksCleanUpGlueMetastore.class);
    private static final String TEST_DATABASE_NAME_PREFIX = "test_";

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testCleanupOrphanedDatabases()
    {
        GlueClient glueClient = GlueClient.create();
        long creationTimeMillisThreshold = currentTimeMillis() - DAYS.toMillis(1);
        List<String> orphanedDatabases = glueClient.getDatabasesPaginator(_ -> {}).stream()
                .map(GetDatabasesResponse::databaseList)
                .flatMap(List::stream)
                .filter(database -> isOrphanedTestDatabase(database, creationTimeMillisThreshold))
                .map(Database::name)
                .toList();

        if (!orphanedDatabases.isEmpty()) {
            log.info("Found %s %s* databases that look orphaned, removing", orphanedDatabases.size(), TEST_DATABASE_NAME_PREFIX);
            orphanedDatabases.forEach(database -> {
                try {
                    log.info("Deleting %s database", database);
                    glueClient.deleteDatabase(builder -> builder.name(database));
                }
                catch (EntityNotFoundException e) {
                    log.info("Database [%s] not found, could be removed by other cleanup process", database);
                }
                catch (RuntimeException e) {
                    log.warn(e, "Failed to remove database [%s]", database);
                }
            });
        }
    }

    private static boolean isOrphanedTestDatabase(Database database, long creationTimeMillisThreshold)
    {
        return database.name().startsWith(TEST_DATABASE_NAME_PREFIX) &&
                database.createTime().toEpochMilli() <= creationTimeMillisThreshold;
    }
}
