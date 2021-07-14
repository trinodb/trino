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

package io.trino.plugin.hive;

import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.io.IOException;
import java.util.Iterator;

import static io.trino.plugin.hive.HiveSessionProperties.isHudiMetadataVerificationEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isPreferMetadataToListHudiFiles;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;

public final class HudiDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(HudiDirectoryLister.class);

    private final HoodieTableFileSystemView fileSystemView;

    public HudiDirectoryLister(Configuration conf,
            ConnectorSession session,
            Table table)
    {
        log.info("Using Hudi Directory Lister.");
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(conf)
                .setBasePath(table.getStorage().getLocation())
                .build();
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(isPreferMetadataToListHudiFiles(session))
                .validate(isHudiMetadataVerificationEnabled(session))
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient,
                metadataConfig);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        log.debug("Listing path using Hudi directory lister: %s", path.toString());
        return new HudiFileInfoIterator(fileSystemView, table.getStorage().getLocation(), path);
    }

    public static class HudiFileInfoIterator
            implements RemoteIterator<LocatedFileStatus>
    {
        private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;

        public HudiFileInfoIterator(HoodieTableFileSystemView fileSystemView, String tablePath, Path directory)
        {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), directory);
            this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
        }

        @Override
        public boolean hasNext()
        {
            return hoodieBaseFileIterator.hasNext();
        }

        @Override
        public LocatedFileStatus next()
                throws IOException
        {
            FileStatus fileStatus = hoodieBaseFileIterator.next().getFileStatus();
            String[] name = new String[] {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = new String[] {"localhost"};
            return new LocatedFileStatus(fileStatus,
                    new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())});
        }
    }
}
