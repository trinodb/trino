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
package io.trino.plugin.deltalake.metastore.glue;

import io.airlift.log.Logger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class TestDeltaLakeCleanUpGlueMetastore
{
    private static final Logger log = Logger.get(TestDeltaLakeCleanUpGlueMetastore.class);

    private static final String TEST_DATABASE_NAME_PREFIX = "test_";
    private static final Duration CLEANUP_THRESHOLD = Duration.ofDays(1);

    @Test
    public void cleanupOrphanedDatabases()
    {
        GlueClient glueClient = GlueClient.create();
        Instant creationTimeThreshold = Instant.now().minus(CLEANUP_THRESHOLD);
        List<String> orphanedDatabases = glueClient.getDatabasesPaginator(_ -> {}).stream()
                .map(GetDatabasesResponse::databaseList)
                .flatMap(List::stream)
                .filter(database -> database.name().startsWith(TEST_DATABASE_NAME_PREFIX))
                .filter(database -> database.createTime().isBefore(creationTimeThreshold))
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
}
