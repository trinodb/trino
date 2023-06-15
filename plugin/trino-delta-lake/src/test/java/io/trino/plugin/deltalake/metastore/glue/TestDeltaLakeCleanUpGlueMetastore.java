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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;

import java.util.List;

import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncPaginatedRequest;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncRequest;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestDeltaLakeCleanUpGlueMetastore
{
    private static final Logger log = Logger.get(TestDeltaLakeCleanUpGlueMetastore.class);

    private static final String TEST_DATABASE_NAME_PREFIX = "test_";

    @Test
    public void cleanupOrphanedDatabases()
    {
        GlueAsyncClient glueClient = GlueAsyncClient.builder().build();
        long creationTimeMillisThreshold = currentTimeMillis() - DAYS.toMillis(1);
        ImmutableList.Builder<String> databaseNames = ImmutableList.builder();
        awsSyncPaginatedRequest(glueClient.getDatabasesPaginator(GetDatabasesRequest.builder().build()),
                getDatabasesResponse -> getDatabasesResponse.databaseList()
                        .stream()
                        .filter(glueDatabase -> glueDatabase.name().startsWith(TEST_DATABASE_NAME_PREFIX)
                                && glueDatabase.createTime().toEpochMilli() <= creationTimeMillisThreshold)
                        .forEach(glueDatabase -> {
                            databaseNames.add(glueDatabase.name());
                        }),
                new AwsApiCallStats());
        List<String> orphanedDatabases = databaseNames.build();

        if (!orphanedDatabases.isEmpty()) {
            log.info("Found %s %s* databases that look orphaned, removing", orphanedDatabases.size(), TEST_DATABASE_NAME_PREFIX);
            orphanedDatabases.forEach(database -> {
                try {
                    log.info("Deleting %s database", database);
                    awsSyncRequest(glueClient::deleteDatabase, DeleteDatabaseRequest.builder()
                            .name(database).build(), null);
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
