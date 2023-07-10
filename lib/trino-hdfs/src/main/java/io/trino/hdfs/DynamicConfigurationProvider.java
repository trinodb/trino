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

import java.net.URI;

import static io.trino.hdfs.TrinoFileSystemCache.CACHE_KEY;

public interface DynamicConfigurationProvider
{
    void updateConfiguration(Configuration configuration, HdfsContext context, URI uri);

    /**
     * Set a cache key to invalidate the file system on credential (or other configuration) change.
     */
    static void setCacheKey(Configuration configuration, String value)
    {
        configuration.set(CACHE_KEY, configuration.get(CACHE_KEY, "") + "|" + value);
    }
}
