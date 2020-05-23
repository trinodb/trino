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
package io.prestosql.tests.hive;

import io.airlift.units.Duration;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.Random;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.testing.assertions.Assert.assertEventually;
import static io.prestosql.tests.TestGroups.HIVE_CACHING;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveCaching
        extends ProductTest
{
    @Test(groups = {HIVE_CACHING, PROFILE_SPECIFIC_TESTS})
    public void testReadFromCache()
    {
        testReadFromTable("table1");
        testReadFromTable("table2");
    }

    private void testReadFromTable(String tableNameSuffix)
    {
        String cachedTableName = "hive.default.test_cache_read" + tableNameSuffix;
        String nonCachedTableName = "hivenoncached.default.test_cache_read" + tableNameSuffix;

        String tableData = createTestTable(nonCachedTableName);

        QueryResult beforeCacheStats = getCacheStats();
        long initialRemoteReads = getRemoteReads(beforeCacheStats);
        long initialCachedReads = getCachedReads(beforeCacheStats);
        long initialAsyncDownloadedMb = getAsyncDownloadedMb();

        assertThat(query("SELECT * FROM " + cachedTableName))
                .containsExactly(row(tableData));

        assertEventually(
                new Duration(20, SECONDS),
                () -> {
                    // first query via caching catalog should fetch remote data
                    QueryResult afterQueryCacheStats = getCacheStats();
                    assertGreaterThanOrEqual(getAsyncDownloadedMb(), initialAsyncDownloadedMb + 1);
                    assertGreaterThan(getRemoteReads(afterQueryCacheStats), initialRemoteReads);
                    assertEquals(getCachedReads(afterQueryCacheStats), initialCachedReads);
                });

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    QueryResult beforeQueryCacheStats = getCacheStats();
                    long beforeQueryRemoteReads = getRemoteReads(beforeQueryCacheStats);
                    long beforeQueryCachedReads = getCachedReads(beforeQueryCacheStats);

                    assertThat(query("SELECT * FROM " + cachedTableName))
                            .containsExactly(row(tableData));

                    // query via caching catalog should read exclusively from cache
                    QueryResult afterQueryCacheStats = getCacheStats();
                    assertEquals(getRemoteReads(afterQueryCacheStats), beforeQueryRemoteReads);
                    assertGreaterThan(getCachedReads(afterQueryCacheStats), beforeQueryCachedReads);
                });

        query("DROP TABLE " + nonCachedTableName);
    }

    /**
     * Creates table with text files that are larger than 1MB
     */
    private String createTestTable(String tableName)
    {
        StringBuilder randomDataBuilder = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < 500_000; ++i) {
            randomDataBuilder.append(random.nextInt(10));
        }
        String randomData = randomDataBuilder.toString();

        query("DROP TABLE IF EXISTS " + tableName);
        // use `format` to overcome SQL query length limit
        query("CREATE TABLE " + tableName + " WITH (format='TEXTFILE') AS SELECT format('%1$s%1$s%1$s%1$s%1$s', '" + randomData + "') as col");

        return randomData.repeat(5);
    }

    private QueryResult getCacheStats()
    {
        return query("SELECT cachedreads, remotereads FROM " +
                "jmx.current.\"rubix:catalog=hive,name=stats\" WHERE node = 'presto-worker'");
    }

    private long getCachedReads(QueryResult queryResult)
    {
        return (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("cachedreads").get() - 1);
    }

    private long getRemoteReads(QueryResult queryResult)
    {
        return (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("remotereads").get() - 1);
    }

    private long getAsyncDownloadedMb()
    {
        return (Long) getOnlyElement(query("SELECT Count FROM " +
                "jmx.current.\"metrics:name=rubix.bookkeeper.count.async_downloaded_mb\" WHERE node = 'presto-worker'").rows())
                .get(0);
    }
}
