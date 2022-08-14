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
package io.trino.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestTrinoFileSystemCacheStats
{
    @Test
    public void testCacheSizeIsCorrect()
            throws Exception
    {
        TrinoFileSystemCache trinoFileSystemCache = new TrinoFileSystemCache();
        TrinoFileSystemCacheStats trinoFileSystemCacheStats = trinoFileSystemCache.getFileSystemCacheStats();
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 0);
        assertEquals(trinoFileSystemCache.getCacheSize(), 0);

        Configuration configuration = newEmptyConfiguration();
        trinoFileSystemCache.get(new URI("file:///tmp/path/"), configuration);
        assertEquals(trinoFileSystemCacheStats.getGetCalls().getTotalCount(), 1);
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 1);
        assertEquals(trinoFileSystemCache.getCacheSize(), 1);

        trinoFileSystemCache.get(new URI("file:///tmp/path1/"), configuration);
        assertEquals(trinoFileSystemCacheStats.getGetCalls().getTotalCount(), 2);
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 1);
        assertEquals(trinoFileSystemCache.getCacheSize(), 1);

        // use getUnique to ensure cache size is increased
        FileSystem fileSystem = trinoFileSystemCache.getUnique(new URI("file:///tmp/path2/"), configuration);
        assertEquals(trinoFileSystemCacheStats.getGetCalls().getTotalCount(), 2);
        assertEquals(trinoFileSystemCacheStats.getGetUniqueCalls().getTotalCount(), 1);
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 2);
        assertEquals(trinoFileSystemCache.getCacheSize(), 2);

        trinoFileSystemCache.remove(fileSystem);
        assertEquals(trinoFileSystemCacheStats.getRemoveCalls().getTotalCount(), 1);
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 1);
        assertEquals(trinoFileSystemCache.getCacheSize(), 1);

        trinoFileSystemCache.closeAll();
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 0);
        assertEquals(trinoFileSystemCache.getCacheSize(), 0);
    }

    @Test
    public void testFailedCallsCountIsCorrect()
    {
        TrinoFileSystemCache trinoFileSystemCache = new TrinoFileSystemCache();
        TrinoFileSystemCacheStats trinoFileSystemCacheStats = trinoFileSystemCache.getFileSystemCacheStats();
        Configuration configuration = newEmptyConfiguration();
        configuration.setInt("fs.cache.max-size", 0);
        assertThatThrownBy(() -> trinoFileSystemCache.get(new URI("file:///tmp/path/"), configuration))
                .hasMessageMatching("FileSystem max cache size has been reached: 0");
        assertEquals(trinoFileSystemCacheStats.getGetCallsFailed().getTotalCount(), 1);
        assertEquals(trinoFileSystemCacheStats.getGetCalls().getTotalCount(), 1);
        assertEquals(trinoFileSystemCacheStats.getCacheSize(), 0);
        assertEquals(trinoFileSystemCache.getCacheSize(), 0);
    }
}
