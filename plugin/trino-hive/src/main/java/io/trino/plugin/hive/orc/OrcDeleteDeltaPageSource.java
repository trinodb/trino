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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.NameBasedFieldMapper;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcReader.ProjectedLayout.fullyProjectedLayout;
import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_BUCKET;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_ORIGINAL_TRANSACTION;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_ROW_ID;
import static io.trino.plugin.hive.orc.OrcPageSource.handleException;
import static io.trino.plugin.hive.orc.OrcPageSourceFactory.verifyAcidSchema;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class OrcDeleteDeltaPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;
    private final FileFormatDataSourceStats stats;
    private final AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

    private boolean closed;

    public static Optional<ConnectorPageSource> createOrcDeleteDeltaPageSource(
            Path path,
            long fileSize,
            OrcReaderOptions options,
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, openError(e, path), e);
        }

        try {
            Optional<OrcReader> orcReader = createOrcReader(orcDataSource, options);
            if (orcReader.isPresent()) {
                return Optional.of(new OrcDeleteDeltaPageSource(path, fileSize, orcReader.get(), orcDataSource, stats));
            }
            return Optional.empty();
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ex) {
                e.addSuppressed(ex);
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            String message = openError(e, path);
            if (e instanceof BlockMissingException) {
                throw new TrinoException(HIVE_MISSING_DATA, message, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private OrcDeleteDeltaPageSource(
            Path path,
            long fileSize,
            OrcReader reader,
            OrcDataSource orcDataSource,
            FileFormatDataSourceStats stats)
            throws OrcCorruptionException
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        verifyAcidSchema(reader, path);
        Map<String, OrcColumn> acidColumns = uniqueIndex(
                reader.getRootColumn().getNestedColumns(),
                orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));
        List<OrcColumn> rowIdColumns = ImmutableList.of(
                acidColumns.get(ACID_COLUMN_ORIGINAL_TRANSACTION.toLowerCase(ENGLISH)),
                acidColumns.get(ACID_COLUMN_BUCKET.toLowerCase(ENGLISH)),
                acidColumns.get(ACID_COLUMN_ROW_ID.toLowerCase(ENGLISH)));

        recordReader = reader.createRecordReader(
                rowIdColumns,
                ImmutableList.of(BIGINT, INTEGER, BIGINT),
                ImmutableList.of(fullyProjectedLayout(), fullyProjectedLayout(), fullyProjectedLayout()),
                OrcPredicate.TRUE,
                0,
                fileSize,
                UTC,
                systemMemoryContext,
                MAX_BATCH_SIZE,
                exception -> handleException(orcDataSource.getId(), exception),
                NameBasedFieldMapper::create);
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = recordReader.nextPage();
            if (page == null) {
                close();
            }
            return page;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw handleException(orcDataSource.getId(), e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcDataSource", orcDataSource.getId())
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    private static String openError(Throwable t, Path path)
    {
        return format("Error opening Hive delete delta file %s: %s", path, t.getMessage());
    }
}
