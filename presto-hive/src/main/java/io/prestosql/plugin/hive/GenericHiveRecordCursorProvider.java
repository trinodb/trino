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
package io.prestosql.plugin.hive;

import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.util.HiveUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

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
    public Optional<RecordCursor> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean s3SelectPushdownEnabled)
    {
        configuration.setInt(LineRecordReader.MAX_LINE_LENGTH, textMaxLineLengthBytes);

        // make sure the FileSystem is created with the proper Configuration object
        try {
            this.hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + path, e);
        }

        return hdfsEnvironment.doAs(session.getUser(), () -> {
            RecordReader<?, ?> recordReader = HiveUtil.createRecordReader(configuration, path, start, length, schema, columns);

            return Optional.of(new GenericHiveRecordCursor<>(
                    configuration,
                    path,
                    genericRecordReader(recordReader),
                    length,
                    schema,
                    columns,
                    hiveStorageTimeZone));
        });
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }
}
