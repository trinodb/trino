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
package io.trino.filesystem.alluxio;

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;

/**
 * Used to skip caching data on coordinator while still registering alluxio metrics so
 * that JMX queries for metrics can succeed on the coordinator
 */
public class AlluxioCoordinatorNoOpFileSystemCache
        implements TrinoFileSystemCache
{
    @Inject
    public AlluxioCoordinatorNoOpFileSystemCache()
    {
        try {
            CacheManager cacheManager = CacheManager.Factory.create(new InstancedConfiguration(new AlluxioProperties()));
            cacheManager.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return delegate.newInput();
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        return delegate.newStream();
    }

    @Override
    public void expire(Location source)
            throws IOException
    {
    }
}
