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
package io.trino.tests.product.hive.util;

import io.trino.tempto.query.QueryResult;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.query.QueryExecutor.query;

public final class CachingTestUtils
{
    private CachingTestUtils() {}

    public static CacheStats getCacheStats()
    {
        QueryResult queryResult = query("SELECT " +
                "  sum(Cached_rrc_requests) as cachedreads, " +
                "  sum(Remote_rrc_requests + Direct_rrc_requests) as remotereads, " +
                "  sum(Nonlocal_rrc_requests) as nonlocalreads " +
                "FROM jmx.current.\"rubix:catalog=hive,type=detailed,name=stats\";");

        long cachedReads = (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("cachedreads").get() - 1);

        long remoteReads = (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("remotereads").get() - 1);

        long nonLocalReads = (Long) getOnlyElement(queryResult.rows())
                .get(queryResult.tryFindColumnIndex("nonlocalreads").get() - 1);

        long asyncDownloadedMb = (Long) getOnlyElement(query("SELECT sum(Count) FROM " +
                "jmx.current.\"metrics:name=rubix.bookkeeper.count.async_downloaded_mb\"").rows())
                .get(0);

        return new CacheStats(cachedReads, remoteReads, nonLocalReads, asyncDownloadedMb);
    }

    public static class CacheStats
    {
        private final long cachedReads;
        private final long remoteReads;
        private final long nonLocalReads;
        private final long asyncDownloadedMb;

        public CacheStats(long cachedReads, long remoteReads, long nonLocalReads, long asyncDownloadedMb)
        {
            this.cachedReads = cachedReads;
            this.remoteReads = remoteReads;
            this.nonLocalReads = nonLocalReads;
            this.asyncDownloadedMb = asyncDownloadedMb;
        }

        public long getCachedReads()
        {
            return cachedReads;
        }

        public long getRemoteReads()
        {
            return remoteReads;
        }

        public long getNonLocalReads()
        {
            return nonLocalReads;
        }

        public long getAsyncDownloadedMb()
        {
            return asyncDownloadedMb;
        }
    }
}
