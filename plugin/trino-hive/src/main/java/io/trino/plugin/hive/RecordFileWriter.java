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

import io.airlift.units.DataSize;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.parquet.ParquetRecordWriter;
import io.trino.plugin.hive.util.FieldSetterFactory;
import io.trino.plugin.hive.util.FieldSetterFactory.FieldSetter;
import io.trino.plugin.hive.util.TextHeaderWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.HiveWriteUtils.createRecordWriter;
import static io.trino.plugin.hive.util.HiveWriteUtils.getRowColumnInspectors;
import static io.trino.plugin.hive.util.HiveWriteUtils.initializeSerializer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

public class RecordFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(RecordFileWriter.class);

    private final Path path;
    private final JobConf conf;
    private final int fieldCount;
    private final Serializer serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object row;
    private final FieldSetter[] setters;
    private final long estimatedWriterMemoryUsage;

    private boolean committed;
    private long finalWrittenBytes = -1;

    public RecordFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            DataSize estimatedWriterMemoryUsage,
            JobConf conf,
            TypeManager typeManager,
            DateTimeZone parquetTimeZone,
            ConnectorSession session)
    {
        this.path = requireNonNull(path, "path is null");
        this.conf = requireNonNull(conf, "conf is null");

        // existing tables may have columns in a different order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, getTimestampPrecision(session)))
                .collect(toList());

        fieldCount = fileColumnNames.size();

        String serde = storageFormat.getSerde();
        serializer = initializeSerializer(conf, schema, serde);

        List<ObjectInspector> objectInspectors = getRowColumnInspectors(fileColumnTypes);
        tableInspector = getStandardStructObjectInspector(fileColumnNames, objectInspectors);

        if (storageFormat.getOutputFormat().equals(HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS)) {
            Optional<TextHeaderWriter> textHeaderWriter = Optional.of(new TextHeaderWriter(serializer, typeManager, session, fileColumnNames));
            recordWriter = createRecordWriter(path, conf, schema, storageFormat.getOutputFormat(), session, textHeaderWriter);
        }
        else {
            recordWriter = createRecordWriter(path, conf, schema, storageFormat.getOutputFormat(), session, Optional.empty());
        }

        // reorder (and possibly reduce) struct fields to match input
        structFields = inputColumnNames.stream()
                .map(tableInspector::getStructFieldRef)
                .collect(toImmutableList());

        row = tableInspector.create();

        DateTimeZone timeZone = (recordWriter instanceof ParquetRecordWriter) ? parquetTimeZone : DateTimeZone.UTC;
        FieldSetterFactory fieldSetterFactory = new FieldSetterFactory(timeZone);

        setters = new FieldSetter[structFields.size()];
        for (int i = 0; i < setters.length; i++) {
            setters[i] = fieldSetterFactory.create(tableInspector, row, structFields.get(i), fileColumnTypes.get(structFields.get(i).getFieldID()));
        }

        this.estimatedWriterMemoryUsage = estimatedWriterMemoryUsage.toBytes();
    }

    @Override
    public long getWrittenBytes()
    {
        if (recordWriter instanceof ExtendedRecordWriter) {
            return ((ExtendedRecordWriter) recordWriter).getWrittenBytes();
        }

        if (committed) {
            if (finalWrittenBytes != -1) {
                return finalWrittenBytes;
            }

            try {
                finalWrittenBytes = path.getFileSystem(conf).getFileStatus(path).getLen();
                return finalWrittenBytes;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        // there is no good way to get this when RecordWriter is not yet committed
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + estimatedWriterMemoryUsage;
    }

    @Override
    public void appendRows(Page dataPage)
    {
        for (int position = 0; position < dataPage.getPositionCount(); position++) {
            appendRow(dataPage, position);
        }
    }

    public void appendRow(Page dataPage, int position)
    {
        for (int field = 0; field < fieldCount; field++) {
            Block block = dataPage.getBlock(field);
            if (block.isNull(position)) {
                tableInspector.setStructFieldData(row, structFields.get(field), null);
            }
            else {
                setters[field].setField(block, position);
            }
        }

        try {
            recordWriter.write(serializer.serialize(row, tableInspector));
        }
        catch (SerDeException | IOException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            recordWriter.close(false);
            committed = true;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        return createRollbackAction(path, conf);
    }

    @Override
    public void rollback()
    {
        Closeable rollbackAction = createRollbackAction(path, conf);
        try (rollbackAction) {
            recordWriter.close(true);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    private static Closeable createRollbackAction(Path path, JobConf conf)
    {
        return () -> path.getFileSystem(conf).delete(path, false);
    }

    @Override
    public long getValidationCpuNanos()
    {
        // RecordFileWriter delegates to Hive RecordWriter and there is no validation
        return 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .toString();
    }

    public interface ExtendedRecordWriter
            extends RecordWriter
    {
        long getWrittenBytes();
    }
}
