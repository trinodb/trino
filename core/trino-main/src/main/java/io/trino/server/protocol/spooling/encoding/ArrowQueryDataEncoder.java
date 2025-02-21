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
package io.trino.server.protocol.spooling.encoding;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.arrow.PageWriter;
import io.trino.server.protocol.spooling.encoding.arrow.TrinoToArrowTypeConverter;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.encoding.arrow.TrinoToArrowTypeConverter.toArrowField;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArrowQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String ENCODING = "arrow";

    private final List<Field> fields;
    private final List<OutputColumn> columns;
    private final CompressionCodec.Factory compressionFactory;
    private final CompressionUtil.CodecType codecType;
    private final BufferAllocator allocator;

    public ArrowQueryDataEncoder(
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory,
            CompressionUtil.CodecType codecType,
            List<OutputColumn> columns)
    {
        this.fields = columns.stream()
                .map(column -> toArrowField(column.columnName(), column.type(), true))
                .collect(toImmutableList());
        this.columns = requireNonNull(columns, "columns is null");

        this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.codecType = requireNonNull(codecType, "codecType is null");
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        try (VectorSchemaRoot schema = VectorSchemaRoot.create(new Schema(fields), allocator)) {
            ArrowStreamWriter streamWriter = new ArrowStreamWriter(schema, null, Channels.newChannel(output), IpcOption.DEFAULT, compressionFactory, codecType);
            try (PageWriter arrowPageWriter = new PageWriter(streamWriter, schema, columns)) {
                return DataAttributes.builder()
                        .set(SEGMENT_SIZE, toIntExact(arrowPageWriter.writePages(pages)))
                        .build();
            }
        }
    }

    @Override
    public String encoding()
    {
        return switch (codecType) {
            case NO_COMPRESSION -> ENCODING;
            case ZSTD -> ENCODING + "+zstd";
            default -> throw new IllegalArgumentException("Unsupported codec type: " + codecType);
        };
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;

        @Inject
        public Factory(BufferAllocator rootAllocator)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
        }

        @Override
        public List<OutputColumn> unsupported(List<OutputColumn> columns)
        {
            return TrinoToArrowTypeConverter.unsupported(columns);
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE),
                    NoCompressionCodec.Factory.INSTANCE,
                    CompressionUtil.CodecType.NO_COMPRESSION,
                    columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;
        private final CompressionCodec.Factory compressionFactory;

        @Inject
        public ZstdFactory(BufferAllocator rootAllocator, CompressionCodec.Factory compressionFactory)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
            this.compressionFactory = requireNonNull(compressionFactory, "compressionFactory is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE),
                    compressionFactory,
                    CompressionUtil.CodecType.ZSTD,
                    columns);
        }

        @Override
        public List<OutputColumn> unsupported(List<OutputColumn> columns)
        {
            return TrinoToArrowTypeConverter.unsupported(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING + "+zstd";
        }
    }
}
