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
package io.trino.plugin.hudi.query;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.query.index.HudiIndexSupport;
import io.trino.plugin.hudi.query.index.IndexSupportFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiUtil.getFileSystemView;

public class HudiSnapshotDirectoryLister
        implements HudiDirectoryLister
{
    private static final Logger log = Logger.get(HudiSnapshotDirectoryLister.class);
    private final HudiTableHandle tableHandle;
    private final Lazy<HoodieTableFileSystemView> lazyFileSystemView;
    private final Optional<HudiIndexSupport> indexSupportOpt;

    public HudiSnapshotDirectoryLister(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            boolean enableMetadataTable,
            Lazy<HoodieTableMetadata> lazyTableMetadata)
    {
        this.tableHandle = tableHandle;
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        this.lazyFileSystemView = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            HoodieTableMetaClient metaClient = tableHandle.getMetaClient();
            HoodieTableFileSystemView fileSystemView = getFileSystemView(lazyTableMetadata.get(), metaClient);
            if (enableMetadataTable) {
                fileSystemView.loadAllPartitions();
            }
            log.info("Created file system view of table %s in %s ms", schemaTableName, timer.endTimer());
            return fileSystemView;
        });

        Lazy<HoodieTableMetaClient> lazyMetaClient = Lazy.lazily(tableHandle::getMetaClient);
        this.indexSupportOpt = enableMetadataTable ?
                IndexSupportFactory.createIndexSupport(tableHandle, lazyMetaClient, lazyTableMetadata, tableHandle.getRegularPredicates(), session) : Optional.empty();
    }

    @Override
    public List<FileSlice> listStatus(HudiPartitionInfo partitionInfo, boolean useIndex)
    {
        HoodieTimer timer = HoodieTimer.start();
        Stream<FileSlice> slices = lazyFileSystemView.get().getLatestFileSlicesBeforeOrOn(
                partitionInfo.getRelativePartitionPath(),
                tableHandle.getLatestCommitTime(),
                false);

        if (!useIndex) {
            return slices.collect(toImmutableList());
        }

        ImmutableList<FileSlice> collect = slices
                .filter(slice -> indexSupportOpt
                        .map(indexSupport -> !indexSupport.shouldSkipFileSlice(slice))
                        .orElse(true))
                .collect(toImmutableList());
        log.info("Listed partition [%s] on table %s.%s in %s ms",
                partitionInfo, tableHandle.getSchemaName(), tableHandle.getTableName(), timer.endTimer());
        return collect;
    }

    @Override
    public void close()
    {
        if (!lazyFileSystemView.get().isClosed()) {
            lazyFileSystemView.get().close();
        }
    }
}
