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

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.page.HudiPageSourceCreator;
import io.trino.plugin.hudi.page.HudiPageSourceFactory;
import io.trino.plugin.hudi.page.HudiParquetPageSourceCreator;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiUtil.convertPartitionValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HudiConfig hudiConfig;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final DateTimeZone timeZone;
    private final Map<HoodieFileFormat, HudiPageSourceCreator> pageSourceBuilderMap;
    private final Map<String, Object> context;

    @Inject
    public HudiPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            HudiConfig hudiConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.hudiConfig = requireNonNull(hudiConfig, "hudiConfig is null");
        this.timeZone = DateTimeZone.forID(TimeZone.getDefault().getID());
        this.pageSourceBuilderMap = new HashMap<>();
        this.context = ImmutableMap.<String, Object>builder()
                .put(
                        HudiParquetPageSourceCreator.CONTEXT_KEY_PARQUET_READER_OPTIONS,
                        requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions())
                .buildOrThrow();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HudiSplit split = (HudiSplit) connectorSplit;
        Path path = new Path(split.getPath());
        HoodieFileFormat hudiFileFormat = HudiUtil.getHudiFileFormat(path.toString());
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        // just send regular columns to create parquet page source
        // for partition columns, separate blocks will be created
        List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey())
                .collect(Collectors.toList());
        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), path);
        ConnectorPageSource dataPageSource = getHudiPageSourceCreator(hudiFileFormat).createPageSource(
                configuration, session.getIdentity(), regularColumns, split);

        return new HudiPageSource(
                hiveColumns,
                convertPartitionValues(hiveColumns, split.getPartitionKeys()), // create blocks for partition values
                dataPageSource);
    }

    private Map<String, Block> convertPartitionValues(
            List<HiveColumnHandle> allColumns,
            List<HivePartitionKey> partitionKeys)
    {
        return allColumns.stream()
                .filter(HiveColumnHandle::isPartitionKey)
                .collect(toMap(
                        HiveColumnHandle::getName,
                        columnHandle -> Utils.nativeValueToBlock(
                                columnHandle.getType(),
                                convertPartitionValue(
                                        columnHandle.getName(),
                                        partitionKeys.get(0).getValue(),
                                        columnHandle.getType().getTypeSignature()).orElse(null))));
    }

    private HudiPageSourceCreator getHudiPageSourceCreator(HoodieFileFormat hudiFileFormat)
    {
        if (!pageSourceBuilderMap.containsKey(hudiFileFormat)) {
            // HudiPageSourceProvider::createPageSource may be called concurrently
            // So the below guarantees the construction of HudiPageSourceCreator once
            synchronized (pageSourceBuilderMap) {
                pageSourceBuilderMap.computeIfAbsent(hudiFileFormat,
                        format -> HudiPageSourceFactory.get(
                                format, hudiConfig, hdfsEnvironment, fileFormatDataSourceStats, timeZone, context));
            }
        }
        return pageSourceBuilderMap.get(hudiFileFormat);
    }
}
