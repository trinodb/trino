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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MetaDirStatisticsAccess
        implements ExtendedStatisticsAccess
{
    private static final String STATISTICS_META_DIR = TRANSACTION_LOG_DIRECTORY + "/_trino_meta"; // store inside TL directory so it is not deleted by VACUUM
    private static final String STATISTICS_FILE = "extended_stats.json";

    private static final String STARBURST_META_DIR = TRANSACTION_LOG_DIRECTORY + "/_starburst_meta";
    private static final String STARBURST_STATISTICS_FILE = "extendeded_stats.json";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<ExtendedStatistics> statisticsCodec;

    @Inject
    public MetaDirStatisticsAccess(
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<ExtendedStatistics> statisticsCodec)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.statisticsCodec = requireNonNull(statisticsCodec, "statisticsCodec is null");
    }

    @Override
    public Optional<ExtendedStatistics> readExtendedStatistics(
            ConnectorSession session,
            String tableLocation)
    {
        return readExtendedStatistics(session, tableLocation, STATISTICS_META_DIR, STATISTICS_FILE)
                .or(() -> readExtendedStatistics(session, tableLocation, STARBURST_META_DIR, STARBURST_STATISTICS_FILE));
    }

    private Optional<ExtendedStatistics> readExtendedStatistics(ConnectorSession session, String tableLocation, String statisticsDirectory, String statisticsFile)
    {
        try {
            Path statisticsPath = new Path(new Path(tableLocation, statisticsDirectory), statisticsFile);
            TrinoInputFile inputFile = fileSystemFactory.create(session).newInputFile(statisticsPath.toString());
            if (!inputFile.exists()) {
                return Optional.empty();
            }

            try (InputStream inputStream = inputFile.newStream()) {
                return Optional.of(statisticsCodec.fromJson(inputStream.readAllBytes()));
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("failed to read statistics with table location %s", tableLocation), e);
        }
    }

    @Override
    public void updateExtendedStatistics(
            ConnectorSession session,
            String tableLocation,
            ExtendedStatistics statistics)
    {
        Path metaPath = new Path(tableLocation, STATISTICS_META_DIR);
        try {
            Path statisticsPath = new Path(metaPath, STATISTICS_FILE);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try (OutputStream outputStream = fileSystem.newOutputFile(statisticsPath.toString()).createOrOverwrite()) {
                outputStream.write(statisticsCodec.toJsonBytes(statistics));
            }

            // Remove outdated Starburst stats file, if it exists.
            String starburstStatisticsPath = new Path(new Path(tableLocation, STARBURST_META_DIR), STARBURST_STATISTICS_FILE).toString();
            if (fileSystem.newInputFile(starburstStatisticsPath).exists()) {
                fileSystem.deleteFile(new Path(new Path(tableLocation, STARBURST_META_DIR), STARBURST_STATISTICS_FILE).toString());
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("failed to store statistics with table location %s", tableLocation), e);
        }
    }

    @Override
    public void deleteExtendedStatistics(ConnectorSession session, String tableLocation)
    {
        Path statisticsPath = new Path(new Path(tableLocation, STATISTICS_META_DIR), STATISTICS_FILE);
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            if (fileSystem.newInputFile(statisticsPath.toString()).exists()) {
                fileSystem.deleteFile(statisticsPath.toString());
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Error deleting statistics file %s", statisticsPath), e);
        }
    }
}
