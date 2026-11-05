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

import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergInternalDataReadBuilder.checkSupported;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.IcebergParquetSchemaConverter.convert;
import static io.trino.plugin.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * Writes Iceberg internal data, like partition statistics files, with Trino's Parquet writer. Records are
 * {@link StructLike}s holding Iceberg's internal representation of values.
 */
final class IcebergInternalDataWriteBuilder
        implements InternalData.WriteBuilder
{
    private final OutputFile outputFile;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private Schema schema;
    private String name = "table";

    IcebergInternalDataWriteBuilder(OutputFile outputFile, TypeManager typeManager, NodeVersion nodeVersion)
    {
        this.outputFile = requireNonNull(outputFile, "outputFile is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    @Override
    public InternalData.WriteBuilder schema(Schema schema)
    {
        this.schema = requireNonNull(schema, "schema is null");
        return this;
    }

    @Override
    public InternalData.WriteBuilder named(String name)
    {
        this.name = requireNonNull(name, "name is null");
        return this;
    }

    @Override
    public InternalData.WriteBuilder set(String property, String value)
    {
        // Trino's writer defaults apply; the library sets no writer properties for partition statistics files
        return this;
    }

    @Override
    public InternalData.WriteBuilder meta(String property, String value)
    {
        throw new UnsupportedOperationException("Trino's Parquet writer does not write file metadata: " + property);
    }

    @Override
    public InternalData.WriteBuilder overwrite()
    {
        // Trino file systems create a file the same way whether it exists or not, see ForwardingOutputFile.createOrOverwrite
        return this;
    }

    @Override
    public <D> FileAppender<D> build()
            throws IOException
    {
        checkState(schema != null, "schema is not set");
        checkSupported(schema.asStruct());
        if (!(outputFile instanceof ForwardingOutputFile forwardingOutputFile)) {
            throw new UnsupportedOperationException("Trino writes Parquet internal data only to its own file systems: " + outputFile);
        }
        TrinoOutputFile file = forwardingOutputFile.outputFile();
        List<String> names = schema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> types = schema.columns().stream()
                .map(field -> toTrinoType(field.type(), typeManager))
                .collect(toImmutableList());
        OutputStream outputStream = file.create();
        try {
            ParquetWriter writer = new ParquetWriter(
                    outputStream,
                    convert(schema, name),
                    makeTypeMap(types, names),
                    ParquetWriterOptions.builder().build(),
                    CompressionCodec.ZSTD,
                    nodeVersion.toString(),
                    Optional.empty(),
                    Optional.empty());
            return new PageAppender<>(schema, types, writer, outputStream);
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, outputStream);
        }
    }

    private static final class PageAppender<D>
            implements FileAppender<D>
    {
        private final List<Types.NestedField> fields;
        private final List<Type> types;
        private final PageBuilder pageBuilder;
        private final ParquetWriter writer;
        // closing it releases the file without finishing it
        private final OutputStream outputStream;
        private long recordCount;
        private boolean closed;
        private boolean failed;

        private PageAppender(Schema schema, List<Type> types, ParquetWriter writer, OutputStream outputStream)
        {
            this.fields = schema.columns();
            this.types = types;
            this.pageBuilder = new PageBuilder(types);
            this.writer = writer;
            this.outputStream = outputStream;
        }

        @Override
        public void add(D datum)
        {
            checkState(!closed, "appender is closed");
            checkState(!failed, "appender failed to add a record");
            try {
                StructLike record = (StructLike) datum;
                for (int i = 0; i < fields.size(); i++) {
                    writeValue(types.get(i), fields.get(i).type(), pageBuilder.getBlockBuilder(i), record.get(i, Object.class));
                }
                pageBuilder.declarePosition();
                recordCount++;
                if (pageBuilder.isFull()) {
                    flush();
                }
            }
            catch (RuntimeException e) {
                // the page builder is left half written
                failed = true;
                throw e;
            }
        }

        @Override
        public Metrics metrics()
        {
            checkState(closed, "appender is not closed");
            return new Metrics(recordCount, null, null, null, null);
        }

        @Override
        public long length()
        {
            return writer.getEstimatedWrittenBytes();
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            if (failed) {
                // do not finish the file, as it would look valid but miss records
                outputStream.close();
                throw new IOException("Appender failed to add a record, the file is incomplete");
            }
            try (ParquetWriter _ = writer) {
                flush();
            }
        }

        private void flush()
        {
            if (pageBuilder.isEmpty()) {
                return;
            }
            try {
                writer.write(pageBuilder.build());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            pageBuilder.reset();
        }
    }

    private static void writeValue(Type type, org.apache.iceberg.types.Type icebergType, BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }
        if (type instanceof RowType rowType) {
            StructLike struct = (StructLike) value;
            List<Types.NestedField> fields = icebergType.asStructType().fields();
            ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
                for (int i = 0; i < fields.size(); i++) {
                    writeValue(rowType.getFields().get(i).getType(), fields.get(i).type(), fieldBuilders.get(i), struct.get(i, Object.class));
                }
            });
            return;
        }
        writeNativeValue(type, builder, switch (icebergType.typeId()) {
            // internally, strings are any CharSequence, and fixed values are ByteBuffers like binary values
            case STRING -> utf8Slice(value.toString());
            case BINARY, FIXED -> wrappedBuffer(ByteBuffers.toByteArray((ByteBuffer) value));
            default -> convertIcebergValueToTrino(icebergType, value);
        });
    }
}
