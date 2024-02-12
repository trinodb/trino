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
package io.trino.tests.product.utils;

import io.trino.tempto.query.QueryResult;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public final class CachingTestUtils
{
    private CachingTestUtils() {}

    public static CacheStats getCacheStats(String catalog)
    {
        QueryResult queryResult = onTrino().executeQuery("SELECT " +
                "  sum(\"cachereads.alltime.count\") as cachereads, " +
                "  sum(\"externalreads.alltime.count\") as externalreads " +
                "FROM jmx.current.\"io.trino.filesystem.alluxio:name=" + catalog + ",type=alluxiocachestats\";");

        double cacheReads = (Double) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("cachereads").get() - 1);

        double externalReads = (Double) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("externalreads").get() - 1);

        long cacheSpaceUsed = (Long) onTrino().executeQuery("SELECT sum(count) FROM " +
                "jmx.current.\"org.alluxio:name=client.cachespaceusedcount,type=counters\"").getOnlyValue();

        return new CacheStats(cacheReads, externalReads, cacheSpaceUsed);
    }

    public record CacheStats(double cacheReads, double externalReads, long cacheSpaceUsed) {}
}
