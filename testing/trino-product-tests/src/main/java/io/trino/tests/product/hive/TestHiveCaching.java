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
package io.trino.tests.product.hive;

import io.airlift.units.Duration;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tests.product.hive.util.CachingTestUtils.CacheStats;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Random;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_CACHING;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.CachingTestUtils.getCacheStats;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveCaching
        extends ProductTest
{
    private static final int NUMBER_OF_FILES = 5;

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

        Row[] tableData = createTestTable(nonCachedTableName);

        CacheStats beforeCacheStats = getCacheStats();
        long initialRemoteReads = beforeCacheStats.getRemoteReads();
        long initialCachedReads = beforeCacheStats.getCachedReads();
        long initialNonLocalReads = beforeCacheStats.getNonLocalReads();
        long initialAsyncDownloadedMb = beforeCacheStats.getAsyncDownloadedMb();

        assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName))
                .containsExactlyInOrder(tableData);

        assertEventually(
                new Duration(20, SECONDS),
                () -> {
                    // first query via caching catalog should fetch remote data
                    CacheStats afterQueryCacheStats = getCacheStats();
                    assertGreaterThanOrEqual(afterQueryCacheStats.getAsyncDownloadedMb(), initialAsyncDownloadedMb + 5);
                    assertGreaterThan(afterQueryCacheStats.getRemoteReads(), initialRemoteReads);
                    assertEquals(afterQueryCacheStats.getCachedReads(), initialCachedReads);
                    assertEquals(afterQueryCacheStats.getNonLocalReads(), initialNonLocalReads);
                });

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    CacheStats beforeQueryCacheStats = getCacheStats();
                    long beforeQueryCachedReads = beforeQueryCacheStats.getCachedReads();
                    long beforeQueryRemoteReads = beforeQueryCacheStats.getRemoteReads();
                    long beforeQueryNonLocalReads = beforeQueryCacheStats.getNonLocalReads();

                    assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName))
                            .containsExactlyInOrder(tableData);

                    // query via caching catalog should read exclusively from cache
                    CacheStats afterQueryCacheStats = getCacheStats();
                    assertGreaterThan(afterQueryCacheStats.getCachedReads(), beforeQueryCachedReads);
                    assertEquals(afterQueryCacheStats.getRemoteReads(), beforeQueryRemoteReads);
                    // all reads should be local as Trino would schedule splits on nodes with cached data
                    assertEquals(afterQueryCacheStats.getNonLocalReads(), beforeQueryNonLocalReads);
                });

        onTrino().executeQuery("DROP TABLE " + nonCachedTableName);
    }

    /**
     * Creates table with 5 text files that are larger than 1MB
     */
    private Row[] createTestTable(String tableName)
    {
        StringBuilder randomDataBuilder = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < 500_000; ++i) {
            randomDataBuilder.append(random.nextInt(10));
        }
        String randomData = randomDataBuilder.toString();

        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("CREATE TABLE " + tableName + " (col varchar) WITH (format='TEXTFILE')");

        for (int i = 0; i < NUMBER_OF_FILES; ++i) {
            // use `format` to overcome SQL query length limit
            onTrino().executeQuery("INSERT INTO " + tableName + " SELECT format('%1$s%1$s%1$s%1$s%1$s', '" + randomData + "')");
        }

        Row row = row(randomData.repeat(5));
        return Collections.nCopies(NUMBER_OF_FILES, row).toArray(new Row[0]);
    }
}
