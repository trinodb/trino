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

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.util.HiveReaderUtil.createRecordReader;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class GenericHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final int textMaxLineLengthBytes;

    @Inject
    public GenericHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment, HiveConfig config)
    {
        this(hdfsEnvironment, config.getTextMaxLineLength());
    }

    public GenericHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment, DataSize textMaxLineLength)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.textMaxLineLengthBytes = toIntExact(textMaxLineLength.toBytes());
        checkArgument(textMaxLineLengthBytes >= 1, "textMaxLineLength must be at least 1 byte");
    }

    @Override
    public Optional<ReaderRecordCursorWithProjections> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            Location location,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled)
    {
        configuration.setInt(LineRecordReader.MAX_LINE_LENGTH, textMaxLineLengthBytes);

        // make sure the FileSystem is created with the proper Configuration object
        Path path = new Path(location.toString());
        try {
            this.hdfsEnvironment.getFileSystem(session.getIdentity(), path, configuration);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        Optional<ReaderColumns> projections = projectBaseColumns(columns);
        List<HiveColumnHandle> readerColumns = projections
                .map(ReaderColumns::get)
                .map(columnHandles -> columnHandles.stream()
                        .map(HiveColumnHandle.class::cast)
                        .collect(toUnmodifiableList()))
                .orElse(columns);

        RecordCursor cursor = hdfsEnvironment.doAs(session.getIdentity(), () -> {
            RecordReader<?, ?> recordReader = createRecordReader(
                    configuration,
                    path,
                    start,
                    length,
                    schema,
                    readerColumns);

            try {
                return new GenericHiveRecordCursor<>(
                        configuration,
                        path,
                        genericRecordReader(recordReader),
                        length,
                        schema,
                        readerColumns);
            }
            catch (Exception e) {
                try {
                    recordReader.close();
                }
                catch (IOException closeException) {
                    if (e != closeException) {
                        e.addSuppressed(closeException);
                    }
                }
                throw e;
            }
        });

        return Optional.of(new ReaderRecordCursorWithProjections(cursor, projections));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }
}
