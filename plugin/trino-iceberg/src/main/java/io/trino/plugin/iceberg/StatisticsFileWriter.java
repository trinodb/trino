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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.stats.Blob;
import org.apache.iceberg.stats.StatsCompressionCodec;
import org.apache.iceberg.stats.StatsFiles;
import org.apache.iceberg.stats.StatsFormat;
import org.apache.iceberg.stats.StatsWriter;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.trino.plugin.iceberg.Stats.NDV_STATS;
import static io.trino.plugin.iceberg.Stats.longToBytesLittleEndian;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class StatisticsFileWriter
{
    private final String trinoVersion;
    private final FileIoProvider fileIoProvider;

    @Inject
    public StatisticsFileWriter(
            NodeVersion nodeVersion,
            FileIoProvider fileIoProvider)
    {
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
    }

    public StatisticsFile writeStatistics(
            ConnectorSession session,
            IcebergAnalyzeHandle handle,
            Map<String, Integer> columnNameToId,
            long sourceSequenceNumber,
            Collection<ComputedStatistics> computedStatistics)
    {
        String fileName = format("%s-%s.stats", session.getQueryId(), randomUUID());
        LocationProvider locationProvider = getLocationProvider(handle.getSchemaTableName(), handle.getTableLocation(), handle.getStorageProperties());
        String location = locationProvider.newDataLocation(fileName);

        try (FileIO fileIO = fileIoProvider.createFileIo(new HdfsEnvironment.HdfsContext(session), session.getQueryId());
                StatsWriter statsWriter = StatsFiles.write(fileIO.newOutputFile(location))
                        .set(StatsFormat.CREATED_BY_PROPERTY, "Trino version " + trinoVersion)
                        .set("source_sequence_number", Long.toString(sourceSequenceNumber))
                        .build()) {
            for (ComputedStatistics computedStatistic : computedStatistics) {
                verify(computedStatistic.getGroupingColumns().isEmpty() && computedStatistic.getGroupingValues().isEmpty(), "Unexpected grouping");
                verify(computedStatistic.getTableStatistics().isEmpty(), "Unexpected table statistics");
                for (Map.Entry<ColumnStatisticMetadata, Block> entry : computedStatistic.getColumnStatistics().entrySet()) {
                    ColumnStatisticMetadata statisticMetadata = entry.getKey();
                    switch (statisticMetadata.getStatisticType()) {
                        case NUMBER_OF_DISTINCT_VALUES:
                            long ndv = (long) blockToNativeValue(BIGINT, entry.getValue());
                            Integer columnId = verifyNotNull(
                                    columnNameToId.get(statisticMetadata.getColumnName()),
                                    "Column not found in table: [%s]",
                                    statisticMetadata.getColumnName());
                            statsWriter.add(new Blob(NDV_STATS, ImmutableList.of(columnId), longToBytesLittleEndian(ndv), StatsCompressionCodec.NONE));
                            break;

                        case NUMBER_OF_DISTINCT_VALUES_SUMMARY:
                            // TODO: add support for NDV summary/sketch, but using Theta sketch, not HLL; see https://github.com/apache/iceberg-docs/pull/69
                        default:
                            throw new UnsupportedOperationException("Unsupported statistic type: " + statisticMetadata.getStatisticType());
                    }
                }
            }

            statsWriter.finish();
            return new GenericStatisticsFile(
                    location,
                    statsWriter.fileSize(),
                    statsWriter.footerSize(),
                    sourceSequenceNumber,
                    statsWriter.coveredColumns());
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write table statistics", e);
        }
    }
}
