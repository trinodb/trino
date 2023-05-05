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

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.hadoop.AlluxioHdfsInputStream;
import alluxio.hadoop.HdfsFileInputStream;
import alluxio.wire.FileInfo;
import io.trino.filesystem.TrinoInput;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CachingHdfsInputFile
        extends HdfsInputFile
{
    private final CacheManager cacheManager;
    private final AlluxioConfiguration alluxioConf;

    private final FileSystem.Statistics statistics = new FileSystem.Statistics("alluxio");

    public CachingHdfsInputFile(String path, Long length, HdfsEnvironment environment, HdfsContext context,
            CacheManager cacheManager, AlluxioConfiguration alluxioConf)
    {
        super(path, length, environment, context);
        this.cacheManager = cacheManager;
        this.alluxioConf = alluxioConf;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        FileSystem fileSystem = environment.getFileSystem(context, file);
        FSDataInputStream input = environment.doAs(context.getIdentity(), () -> {
            FileInfo info = new FileInfo()
                    .setLastModificationTimeMs(lazyStatus().getModificationTime())
                    .setPath(file.toString())
                    .setFolder(false)
                    .setLength(lazyStatus().getLen());
            String cacheIdentifier = md5().hashString(file.toString() + lazyStatus().getModificationTime(), UTF_8).toString();
            URIStatus uriStatus = new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
            return new FSDataInputStream(new HdfsFileInputStream(
                    new LocalCacheFileInStream(uriStatus, (uri) -> new AlluxioHdfsInputStream(fileSystem.open(file)), cacheManager, alluxioConf),
                    statistics));
        });
        return new HdfsInput(input, this);
    }
}
