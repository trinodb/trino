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

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
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

        query("DROP TABLE IF EXISTS " + nonCachedTableName);
        query("CREATE TABLE " + nonCachedTableName + " WITH (format='ORC') AS SELECT 'Hello world' as col");

        QueryResult beforeCacheStats = getCacheStats();
        long beforeRemoteReads = getRemoteReads(beforeCacheStats);
        long beforeCachedReads = getCachedReads(beforeCacheStats);

        assertThat(query("SELECT * FROM " + cachedTableName))
                .containsExactly(row("Hello world"));

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // first query via caching catalog should fetch remote data
                    QueryResult firstCacheStats = getCacheStats();
                    assertGreaterThan(getRemoteReads(firstCacheStats), beforeRemoteReads);
                    assertEquals(getCachedReads(firstCacheStats), beforeCachedReads);
                });

        QueryResult firstCacheStats = getCacheStats();
        long firstRemoteReads = getRemoteReads(firstCacheStats);
        long firstCachedReads = getCachedReads(firstCacheStats);

        assertThat(query("SELECT * FROM " + cachedTableName))
                .containsExactly(row("Hello world"));

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // second query via caching catalog should read exclusively from cache
                    QueryResult secondCacheStats = getCacheStats();
                    assertEquals(getRemoteReads(secondCacheStats), firstRemoteReads);
                    assertGreaterThan(getCachedReads(secondCacheStats), firstCachedReads);
                });

        query("DROP TABLE " + nonCachedTableName);
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
}
