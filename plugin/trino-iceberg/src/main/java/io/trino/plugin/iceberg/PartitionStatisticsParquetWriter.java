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

import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.IcebergParquetSchemaConverter.convert;
import static io.trino.plugin.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Writes partition statistics rows, which hold Iceberg's internal representation of values, to a Parquet file with
 * Trino's Parquet writer. The Iceberg library cannot write Parquet without {@code parquet-hadoop}.
 */
final class PartitionStatisticsParquetWriter
{
    private PartitionStatisticsParquetWriter() {}

    static void write(OutputFile outputFile, Schema schema, List<GenericRecord> rows, TypeManager typeManager, String trinoVersion)
            throws IOException
    {
        List<Types.NestedField> fields = schema.columns();
        List<String> names = fields.stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> types = fields.stream()
                .map(field -> toTrinoType(field.type(), typeManager))
                .collect(toImmutableList());
        OutputStream outputStream = outputFile.create();
        try (ParquetWriter writer = new ParquetWriter(
                outputStream,
                convert(schema, "table"),
                makeTypeMap(types, names),
                ParquetWriterOptions.builder().build(),
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty())) {
            PageBuilder pageBuilder = new PageBuilder(types);
            for (GenericRecord row : rows) {
                for (int i = 0; i < fields.size(); i++) {
                    writeValue(types.get(i), fields.get(i).type(), pageBuilder.getBlockBuilder(i), row.get(i, Object.class));
                }
                pageBuilder.declarePosition();
                if (pageBuilder.isFull()) {
                    writer.write(pageBuilder.build());
                    pageBuilder.reset();
                }
            }
            if (!pageBuilder.isEmpty()) {
                writer.write(pageBuilder.build());
            }
        }
        catch (IOException | RuntimeException e) {
            // closing the writer on the way out also closed the stream, or the stream was never handed to it
            closeAllSuppress(e, outputStream);
            throw e;
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
