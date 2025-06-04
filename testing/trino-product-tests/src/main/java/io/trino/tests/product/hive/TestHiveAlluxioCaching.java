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
import io.trino.tests.product.utils.CachingTestUtils.CacheStats;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.HIVE_ALLUXIO_CACHING;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.CachingTestUtils.getCacheStats;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveAlluxioCaching
        extends ProductTest
{
    @Test(groups = {HIVE_ALLUXIO_CACHING, PROFILE_SPECIFIC_TESTS})
    public void testReadFromCache()
    {
        testReadFromTable("table1");
        testReadFromTable("table2");
    }

    private void testReadFromTable(String tableNameSuffix)
    {
        String cachedTableName = "hive.default.test_cache_read" + tableNameSuffix;
        String nonCachedTableName = "hivenoncached.default.test_cache_read" + tableNameSuffix;

        createTestTable(nonCachedTableName);

        CacheStats beforeCacheStats = getCacheStats("hive");
        long tableSize = (Long) onTrino().executeQuery("SELECT SUM(size) as size FROM (SELECT \"$path\", \"$file_size\" AS size FROM " + nonCachedTableName + " GROUP BY 1, 2)").getOnlyValue();

        assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName))
                .hasRowsCount(150000);

        assertEventually(
                new Duration(20, SECONDS),
                () -> {
                    // first query via caching catalog should fetch remote data
                    CacheStats afterQueryCacheStats = getCacheStats("hive");
                    assertThat(afterQueryCacheStats.cacheSpaceUsed()).isGreaterThanOrEqualTo(beforeCacheStats.cacheSpaceUsed() + tableSize);
                    assertThat(afterQueryCacheStats.externalReads()).isGreaterThan(beforeCacheStats.externalReads());
                    assertThat(afterQueryCacheStats.cacheReads()).isGreaterThanOrEqualTo(beforeCacheStats.cacheReads());
                });

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    CacheStats beforeQueryCacheStats = getCacheStats("hive");

                    assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName)).hasAnyRows();

                    // query via caching catalog should read exclusively from cache
                    CacheStats afterQueryCacheStats = getCacheStats("hive");
                    assertThat(afterQueryCacheStats.cacheReads()).isGreaterThan(beforeQueryCacheStats.cacheReads());
                    assertThat(afterQueryCacheStats.externalReads()).isEqualTo(beforeQueryCacheStats.externalReads());
                    assertThat(afterQueryCacheStats.cacheSpaceUsed()).isEqualTo(beforeQueryCacheStats.cacheSpaceUsed());
                });

        onTrino().executeQuery("DROP TABLE " + nonCachedTableName);
    }

    /**
     * Creates a table which should contain multiple small ORC files
     */
    private void createTestTable(String tableName)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("SET SESSION hive.target_max_file_size = '4MB'");
        onTrino().executeQuery("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.customer");
    }
}
