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
package io.trino.plugin.deltalake.statistics;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.hive.util.HiveWriteUtils.createDirectory;
import static io.trino.plugin.hive.util.HiveWriteUtils.pathExists;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MetaDirStatisticsAccess
        implements DeltaLakeStatisticsAccess
{
    private static final String STATISTICS_META_DIR = TRANSACTION_LOG_DIRECTORY + "/_trino_meta"; // store inside TL directory so it is not deleted by VACUUM
    private static final String STATISTICS_FILE = "extended_stats.json";

    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<DeltaLakeStatistics> statisticsCodec;

    @Inject
    public MetaDirStatisticsAccess(
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<DeltaLakeStatistics> statisticsCodec)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.statisticsCodec = requireNonNull(statisticsCodec, "statisticsCodec is null");
    }

    @Override
    public Optional<DeltaLakeStatistics> readDeltaLakeStatistics(
            ConnectorSession session,
            String tableLocation)
    {
        try {
            Path metadataDir = new Path(tableLocation, STATISTICS_META_DIR);
            Path statisticsPath = new Path(metadataDir, STATISTICS_FILE);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), statisticsPath);
            if (!fileSystem.exists(statisticsPath)) {
                return Optional.empty();
            }

            try (InputStream inputStream = fileSystem.open(statisticsPath)) {
                return Optional.of(statisticsCodec.fromJson(inputStream.readAllBytes()));
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("failed to read statistics with table location %s", tableLocation), e);
        }
    }

    @Override
    public void updateDeltaLakeStatistics(
            ConnectorSession session,
            String tableLocation,
            DeltaLakeStatistics statistics)
    {
        Path metaPath = new Path(tableLocation, STATISTICS_META_DIR);
        ensureDirectoryExists(session, metaPath);
        try {
            Path statisticsPath = new Path(metaPath, STATISTICS_FILE);

            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), metaPath);
            try (OutputStream outputStream = fileSystem.create(statisticsPath, true)) {
                outputStream.write(statisticsCodec.toJsonBytes(statistics));
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("failed to store statistics with table location %s", tableLocation), e);
        }
    }

    @Override
    public void deleteDeltaLakeStatistics(ConnectorSession session, String tableLocation)
    {
        Path statisticsPath = new Path(new Path(tableLocation, STATISTICS_META_DIR), STATISTICS_FILE);
        try {
            FileSystem hdfs = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), statisticsPath);
            if (!hdfs.delete(statisticsPath, false) && hdfs.exists(statisticsPath)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to delete statistics file %s", statisticsPath));
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Error deleting statistics file %s", statisticsPath), e);
        }
    }

    private void ensureDirectoryExists(ConnectorSession session, Path directoryPath)
    {
        HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(session);
        if (!pathExists(hdfsContext, hdfsEnvironment, directoryPath)) {
            createDirectory(hdfsContext, hdfsEnvironment, directoryPath);
        }
    }
}
