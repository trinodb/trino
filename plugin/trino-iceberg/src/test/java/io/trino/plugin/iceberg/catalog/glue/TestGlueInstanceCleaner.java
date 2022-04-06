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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;

public class TestGlueInstanceCleaner
{
    public static final Logger log = Logger.get(TestGlueInstanceCleaner.class);
    private final GlueMetastoreStats stats = new GlueMetastoreStats();

    @Test
    public void testCleanDatabases()
    {
        AWSGlue glueClient = AWSGlueClientBuilder.standard().withRegion("us-east-2").build();
        long creationTimeMillisThreshold = currentTimeMillis() - DAYS.toMillis(1);
        List<String> databaseNames = getPaginatedResults(
                glueClient::getDatabases,
                new GetDatabasesRequest(),
                GetDatabasesRequest::setNextToken,
                GetDatabasesResult::getNextToken,
                stats.getGetDatabases())
                .map(GetDatabasesResult::getDatabaseList)
                .flatMap(List::stream)
                .filter(glueDatabase -> glueDatabase.getCreateTime().getTime() <= creationTimeMillisThreshold)
                .map(com.amazonaws.services.glue.model.Database::getName)
                .filter(name -> name.startsWith("iceberg_connector_test_") || name.startsWith("iceberg_smoke_test_"))
                .collect(toImmutableList());


        log.info("Remove databases in Glue: %s", databaseNames);
        for (String databaseName : databaseNames) {
            log.info("Removing database in Glue: '%s'", databaseName);
            glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(databaseName));
        }
    }
}
