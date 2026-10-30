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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;
import static io.trino.plugin.iceberg.IcebergPageSourceProvider.createParquetPageSource;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

/**
 * Reads Iceberg internal data, like partition statistics files, with Trino's Parquet reader. Like the library's
 * readers, it produces Iceberg's internal representation of values, in {@link GenericRecord}s unless a custom type is set.
 */
final class IcebergInternalDataReadBuilder
        implements InternalData.ReadBuilder
{
    // the field ID the library uses for the root type
    private static final int ROOT_ID = -1;

    private final InputFile inputFile;
    private final TypeManager typeManager;
    private final Map<Integer, Class<? extends StructLike>> customTypes = new HashMap<>();
    private Schema projection;
    private long start;
    private OptionalLong length = OptionalLong.empty();

    IcebergInternalDataReadBuilder(InputFile inputFile, TypeManager typeManager)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public InternalData.ReadBuilder project(Schema projection)
    {
        this.projection = requireNonNull(projection, "projection is null");
        return this;
    }

    @Override
    public InternalData.ReadBuilder split(long start, long length)
    {
        this.start = start;
        this.length = OptionalLong.of(length);
        return this;
    }

    @Override
    public InternalData.ReadBuilder reuseContainers()
    {
        // every record is a new object, which is allowed
        return this;
    }

    @Override
    public InternalData.ReadBuilder setRootType(Class<? extends StructLike> rootClass)
    {
        customTypes.put(ROOT_ID, requireNonNull(rootClass, "rootClass is null"));
        return this;
    }

    @Override
    public InternalData.ReadBuilder setCustomType(int fieldId, Class<? extends StructLike> structClass)
    {
        customTypes.put(fieldId, requireNonNull(structClass, "structClass is null"));
        return this;
    }

    @Override
    public <D> CloseableIterable<D> build()
    {
        // the library always sets the projection, so the file schema is not converted to an Iceberg schema
        checkState(projection != null, "projection is not set");
        checkSupported(projection.asStruct());
        if (!(inputFile instanceof ForwardingInputFile forwardingInputFile)) {
            throw new UnsupportedOperationException("Trino reads Parquet internal data only from its own file systems: " + inputFile);
        }
        TrinoInputFile file = forwardingInputFile.inputFile();
        List<IcebergColumnHandle> columns = projection.columns().stream()
                .map(field -> getColumnHandle(field, typeManager))
                .collect(toImmutableList());
        List<Type> types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .collect(toImmutableList());
        StructReader reader = new StructReader(ROOT_ID, projection.asStruct(), types);
        return new StructIterable<>(() -> openPageSource(file, columns), reader);
    }

    private ConnectorPageSource openPageSource(TrinoInputFile file, List<IcebergColumnHandle> columns)
    {
        try {
            long fileSize = file.length();
            long splitLength = length.orElse(fileSize - start);
            // the record count, partition and sequence number arguments describe a data file, and only metadata columns,
            // which are not read, use them
            return createParquetPageSource(
                    file,
                    start,
                    splitLength,
                    fileSize,
                    0,
                    0,
                    "",
                    projection,
                    columns,
                    ParquetReaderOptions.defaultOptions(),
                    TupleDomain.all(),
                    1,
                    new FileFormatDataSourceStats(),
                    ParquetFooterCache.noop(),
                    Optional.empty(),
                    "",
                    ImmutableMap.of(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    newSimpleAggregatedMemoryContext(),
                    Optional.empty()).pageSource();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Lists and maps are not supported, the library does not use them in internal data.
     */
    static void checkSupported(Types.StructType struct)
    {
        for (Types.NestedField field : struct.fields()) {
            if (field.type().isStructType()) {
                checkSupported(field.type().asStructType());
            }
            else if (field.type().isNestedType()) {
                throw new UnsupportedOperationException("Unsupported type in internal data: %s: %s".formatted(field.name(), field.type()));
            }
        }
    }

    private static final class StructIterable<D>
            extends CloseableGroup
            implements CloseableIterable<D>
    {
        private final Supplier<ConnectorPageSource> pageSourceFactory;
        private final StructReader reader;

        private StructIterable(Supplier<ConnectorPageSource> pageSourceFactory, StructReader reader)
        {
            this.pageSourceFactory = pageSourceFactory;
            this.reader = reader;
        }

        @Override
        public CloseableIterator<D> iterator()
        {
            StructIterator<D> iterator = new StructIterator<>(pageSourceFactory.get(), reader);
            addCloseable(iterator);
            return iterator;
        }
    }

    private static final class StructIterator<D>
            extends AbstractIterator<D>
            implements CloseableIterator<D>
    {
        private final ConnectorPageSource pageSource;
        private final StructReader reader;
        private SourcePage page;
        private int position;
        private boolean closed;

        private StructIterator(ConnectorPageSource pageSource, StructReader reader)
        {
            this.pageSource = pageSource;
            this.reader = reader;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected D computeNext()
        {
            while (page == null || position == page.getPositionCount()) {
                if (closed || pageSource.isFinished()) {
                    close();
                    return endOfData();
                }
                page = pageSource.getNextSourcePage();
                position = 0;
            }
            return (D) reader.read(page::getBlock, position++);
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Converts the Trino values of a struct to Iceberg values, in a new instance of the struct's type.
     */
    private final class StructReader
    {
        private final Types.StructType struct;
        private final List<Type> fieldTypes;
        private final Optional<Constructor<? extends StructLike>> constructor;
        private final List<Optional<StructReader>> fieldReaders;

        private StructReader(int fieldId, Types.StructType struct, List<Type> fieldTypes)
        {
            this.struct = struct;
            this.fieldTypes = fieldTypes;
            this.constructor = Optional.ofNullable(customTypes.get(fieldId)).map(IcebergInternalDataReadBuilder::structConstructor);
            this.fieldReaders = struct.fields().stream()
                    .map(this::fieldReader)
                    .collect(toImmutableList());
        }

        private Optional<StructReader> fieldReader(Types.NestedField field)
        {
            if (!field.type().isStructType()) {
                return Optional.empty();
            }
            RowType rowType = (RowType) toTrinoType(field.type(), typeManager);
            return Optional.of(new StructReader(field.fieldId(), field.type().asStructType(), rowType.getFieldTypes()));
        }

        public StructLike read(IntFunction<Block> fieldBlocks, int position)
        {
            StructLike value = newStruct();
            for (int field = 0; field < fieldTypes.size(); field++) {
                value.set(field, readField(field, fieldBlocks.apply(field), position));
            }
            return value;
        }

        private Object readField(int field, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }
            Type type = fieldTypes.get(field);
            Optional<StructReader> fieldReader = fieldReaders.get(field);
            if (fieldReader.isPresent()) {
                SqlRow row = ((RowType) type).getObject(block, position);
                return fieldReader.get().read(row::getRawFieldBlock, row.getRawIndex());
            }
            if (type.equals(VARBINARY)) {
                // a copy, so that the value does not retain the page; fixed values are ByteBuffers too
                return ByteBuffer.wrap(VARBINARY.getSlice(block, position).getBytes());
            }
            return getIcebergValue(block, position, type);
        }

        private StructLike newStruct()
        {
            if (constructor.isEmpty()) {
                return GenericRecord.create(struct);
            }
            try {
                return constructor.get().newInstance(struct);
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Cannot create " + constructor.get().getDeclaringClass().getName(), e);
            }
        }
    }

    // like the library's readers, custom types are created with the struct type they are read with
    private static Constructor<? extends StructLike> structConstructor(Class<? extends StructLike> structClass)
    {
        try {
            Constructor<? extends StructLike> constructor = structClass.getDeclaredConstructor(Types.StructType.class);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("Custom type has no constructor taking a struct type: " + structClass.getName(), e);
        }
    }
}
