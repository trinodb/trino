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
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoFileSystemCacheStats
{
    @Test
    public void testCacheSizeIsCorrect()
            throws Exception
    {
        TrinoFileSystemCache trinoFileSystemCache = new TrinoFileSystemCache();
        TrinoFileSystemCacheStats trinoFileSystemCacheStats = trinoFileSystemCache.getStats();
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(0);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(0);

        Configuration configuration = new Configuration(false);
        trinoFileSystemCache.get(new URI("file:///tmp/path/"), configuration);
        assertThat(trinoFileSystemCacheStats.getGetCalls().getTotalCount()).isEqualTo(1);
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(1);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(1);

        trinoFileSystemCache.get(new URI("file:///tmp/path1/"), configuration);
        assertThat(trinoFileSystemCacheStats.getGetCalls().getTotalCount()).isEqualTo(2);
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(1);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(1);

        // use getUnique to ensure cache size is increased
        FileSystem fileSystem = trinoFileSystemCache.getUnique(new URI("file:///tmp/path2/"), configuration);
        assertThat(trinoFileSystemCacheStats.getGetCalls().getTotalCount()).isEqualTo(2);
        assertThat(trinoFileSystemCacheStats.getGetUniqueCalls().getTotalCount()).isEqualTo(1);
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(2);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(2);

        trinoFileSystemCache.remove(fileSystem);
        assertThat(trinoFileSystemCacheStats.getRemoveCalls().getTotalCount()).isEqualTo(1);
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(1);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(1);

        trinoFileSystemCache.closeAll();
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(0);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(0);
    }

    @Test
    public void testFailedCallsCountIsCorrect()
    {
        TrinoFileSystemCache trinoFileSystemCache = new TrinoFileSystemCache();
        TrinoFileSystemCacheStats trinoFileSystemCacheStats = trinoFileSystemCache.getStats();
        Configuration configuration = new Configuration(false);
        configuration.setInt("fs.cache.max-size", 0);
        assertThatThrownBy(() -> trinoFileSystemCache.get(new URI("file:///tmp/path/"), configuration))
                .hasMessageMatching("FileSystem max cache size has been reached: 0");
        assertThat(trinoFileSystemCacheStats.getGetCallsFailed().getTotalCount()).isEqualTo(1);
        assertThat(trinoFileSystemCacheStats.getGetCalls().getTotalCount()).isEqualTo(1);
        assertThat(trinoFileSystemCacheStats.getCacheSize()).isEqualTo(0);
        assertThat(trinoFileSystemCache.getCacheSize()).isEqualTo(0);
    }
}
