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
package io.trino.filesystem.hdfs;

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;

public class CachingHdfsFileSystem
        extends HdfsFileSystem
{
    private final CacheManager cacheManager;
    private final AlluxioConfiguration alluxioConf;

    public CachingHdfsFileSystem(HdfsEnvironment environment,
            HdfsContext context, TrinoHdfsFileSystemStats stats, CacheManager cacheManager, AlluxioConfiguration alluxioConf)
    {
        super(environment, context, stats);
        this.cacheManager = cacheManager;
        this.alluxioConf = alluxioConf;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new CachingHdfsInputFile(location, null, environment, context, stats.getOpenFileCalls(), cacheManager, alluxioConf);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new CachingHdfsInputFile(location, length, environment, context, stats.getOpenFileCalls(), cacheManager, alluxioConf);
    }
}
