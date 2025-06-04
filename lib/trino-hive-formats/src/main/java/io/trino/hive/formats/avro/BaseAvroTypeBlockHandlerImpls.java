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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.annotation.NotThreadSafe;
import io.trino.hive.formats.avro.model.ArrayReadAction;
import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.hive.formats.avro.model.BooleanRead;
import io.trino.hive.formats.avro.model.BytesRead;
import io.trino.hive.formats.avro.model.DoubleRead;
import io.trino.hive.formats.avro.model.EnumReadAction;
import io.trino.hive.formats.avro.model.FixedRead;
import io.trino.hive.formats.avro.model.FloatRead;
import io.trino.hive.formats.avro.model.IntRead;
import io.trino.hive.formats.avro.model.LongRead;
import io.trino.hive.formats.avro.model.MapReadAction;
import io.trino.hive.formats.avro.model.NullRead;
import io.trino.hive.formats.avro.model.ReadErrorReadAction;
import io.trino.hive.formats.avro.model.ReadingUnionReadAction;
import io.trino.hive.formats.avro.model.RecordReadAction;
import io.trino.hive.formats.avro.model.StringRead;
import io.trino.hive.formats.avro.model.WrittenUnionReadAction;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroTypeUtils.isSimpleNullableUnion;
import static io.trino.hive.formats.avro.AvroTypeUtils.unwrapNullableUnion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class BaseAvroTypeBlockHandlerImpls
{
    // same limit as org.apache.avro.io.BinaryDecoder
    static final long MAX_ARRAY_SIZE = (long) Integer.MAX_VALUE - 8L;

    private BaseAvroTypeBlockHandlerImpls() {}

    public static Type baseTypeFor(Schema schema, AvroTypeBlockHandler delegate)
            throws AvroTypeException
    {
        return switch (schema.getType()) {
            case NULL -> throw new UnsupportedOperationException("No null column type support");
            case BOOLEAN -> BooleanType.BOOLEAN;
            case INT -> IntegerType.INTEGER;
            case LONG -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case ENUM, STRING -> VarcharType.VARCHAR;
            case FIXED, BYTES -> VarbinaryType.VARBINARY;
            case ARRAY -> new ArrayType(delegate.typeFor(schema.getElementType()));
            case MAP -> new MapType(VarcharType.VARCHAR, delegate.typeFor(schema.getValueType()), new TypeOperators());
            case RECORD -> {
                ImmutableList.Builder<RowType.Field> rowFieldTypes = ImmutableList.builder();
                for (Schema.Field field : schema.getFields()) {
                    rowFieldTypes.add(new RowType.Field(Optional.of(field.name()), delegate.typeFor(field.schema())));
                }
                yield RowType.from(rowFieldTypes.build());
            }
            case UNION -> {
                if (isSimpleNullableUnion(schema)) {
                    yield delegate.typeFor(unwrapNullableUnion(schema));
                }
                throw new AvroTypeException("Unable to read union with multiple non null types: " + schema);
            }
        };
    }

    public static BlockBuildingDecoder baseBlockBuildingDecoderFor(AvroReadAction action, AvroTypeBlockHandler delegate)
            throws AvroTypeException
    {
        return switch (action) {
            case NullRead __ -> NullBlockBuildingDecoder.INSTANCE;
            case BooleanRead __ -> BooleanBlockBuildingDecoder.INSTANCE;
            case IntRead __ -> IntBlockBuildingDecoder.INSTANCE;
            case LongRead longRead -> new LongBlockBuildingDecoder(longRead.getLongDecoder());
            case FloatRead floatRead -> new FloatBlockBuildingDecoder(floatRead.getFloatDecoder());
            case DoubleRead doubleRead -> new DoubleBlockBuildingDecoder(doubleRead.getDoubleDecoder());
            case BytesRead __ -> BytesBlockBuildingDecoder.INSTANCE;
            case FixedRead __ -> new FixedBlockBuildingDecoder(action.readSchema().getFixedSize());
            case StringRead __ -> StringBlockBuildingDecoder.INSTANCE;
            case ArrayReadAction arrayReadAction -> new ArrayBlockBuildingDecoder(arrayReadAction, delegate);
            case MapReadAction mapReadAction -> new MapBlockBuildingDecoder(mapReadAction, delegate);
            case EnumReadAction enumReadAction -> new EnumBlockBuildingDecoder(enumReadAction);
            case RecordReadAction recordReadAction -> new RowBlockBuildingDecoder(recordReadAction, delegate);
            case ReadingUnionReadAction readingUnionReadAction -> {
                if (!isSimpleNullableUnion(readingUnionReadAction.readSchema())) {
                    throw new AvroTypeException("Unable to natively read into a non nullable union: " + readingUnionReadAction.readSchema());
                }
                yield delegate.blockBuildingDecoderFor(readingUnionReadAction.actualAction());
            }
            case WrittenUnionReadAction writtenUnionReadAction -> {
                // if unionEqiv, then need to check union is
                if (writtenUnionReadAction.unionEqiv() && !isSimpleNullableUnion(writtenUnionReadAction.readSchema())) {
                    throw new AvroTypeException("Unable to natively read into a non nullable union: " + writtenUnionReadAction.readSchema());
                }
                yield new WriterUnionBlockBuildingDecoder(writtenUnionReadAction, delegate);
            }
            case ReadErrorReadAction readErrorReadAction -> throw new AvroTypeException("Incompatible read and write schema returned with error:\n " + readErrorReadAction);
        };
    }

    public static class NullBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        public static final NullBlockBuildingDecoder INSTANCE = new NullBlockBuildingDecoder();

        private NullBlockBuildingDecoder() {}

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readNull();
            builder.appendNull();
        }
    }

    static class BooleanBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private static final BooleanBlockBuildingDecoder INSTANCE = new BooleanBlockBuildingDecoder();

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BOOLEAN.writeBoolean(builder, decoder.readBoolean());
        }
    }

    public static class IntBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private static final IntBlockBuildingDecoder INSTANCE = new IntBlockBuildingDecoder();

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, decoder.readInt());
        }
    }

    public static class LongBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final AvroReadAction.LongIoFunction<Decoder> extractLong;

        public LongBlockBuildingDecoder(AvroReadAction.LongIoFunction<Decoder> extractLong)
        {
            this.extractLong = requireNonNull(extractLong, "extractLong is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BIGINT.writeLong(builder, extractLong.apply(decoder));
        }
    }

    static class FloatBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final AvroReadAction.FloatIoFunction<Decoder> extractFloat;

        public FloatBlockBuildingDecoder(AvroReadAction.FloatIoFunction<Decoder> extractFloat)
        {
            this.extractFloat = requireNonNull(extractFloat, "extractFloat is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            REAL.writeLong(builder, floatToRawIntBits(extractFloat.apply(decoder)));
        }
    }

    static class DoubleBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final AvroReadAction.DoubleIoFunction<Decoder> extractDouble;

        public DoubleBlockBuildingDecoder(AvroReadAction.DoubleIoFunction<Decoder> extractDouble)
        {
            this.extractDouble = requireNonNull(extractDouble, "extractDouble is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            DOUBLE.writeDouble(builder, extractDouble.apply(decoder));
        }
    }

    static class StringBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private static final StringBlockBuildingDecoder INSTANCE = new StringBlockBuildingDecoder();

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            // it is only possible to read String type when underlying write type is String or Bytes
            // both have the same encoding, so coercion is a no-op
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro String with size greater than %s. Found String size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            VARCHAR.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
    }

    static class BytesBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private static final BytesBlockBuildingDecoder INSTANCE = new BytesBlockBuildingDecoder();

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            // it is only possible to read Bytes type when underlying write type is String or Bytes
            // both have the same encoding, so coercion is a no-op
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro Bytes with size greater than %s. Found Bytes size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
    }

    @NotThreadSafe
    static class FixedBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final byte[] bytes;

        public FixedBlockBuildingDecoder(int expectedSize)
        {
            verify(expectedSize >= 0, "expected size must be greater than or equal to 0");
            bytes = new byte[expectedSize];
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readFixed(bytes);
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
    }

    static class EnumBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final List<Slice> symbols;

        public EnumBlockBuildingDecoder(EnumReadAction enumReadAction)
        {
            requireNonNull(enumReadAction, "action is null");
            symbols = enumReadAction.getSymbolIndex();
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeSlice(builder, symbols.get(decoder.readEnum()));
        }
    }

    static class ArrayBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(ArrayReadAction arrayReadAction, AvroTypeBlockHandler typeManager)
                throws AvroTypeException
        {
            requireNonNull(arrayReadAction, "arrayReadAction is null");
            elementBlockBuildingDecoder = typeManager.blockBuildingDecoderFor(arrayReadAction.elementReadAction());
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                long elementsInBlock = decoder.readArrayStart();
                if (elementsInBlock > 0) {
                    do {
                        for (int i = 0; i < elementsInBlock; i++) {
                            elementBlockBuildingDecoder.decodeIntoBlock(decoder, elementBuilder);
                        }
                    }
                    while ((elementsInBlock = decoder.arrayNext()) > 0);
                }
            });
        }
    }

    static class MapBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final BlockBuildingDecoder keyBlockBuildingDecoder = new StringBlockBuildingDecoder();
        private final BlockBuildingDecoder valueBlockBuildingDecoder;

        public MapBlockBuildingDecoder(MapReadAction mapReadAction, AvroTypeBlockHandler typeManager)
                throws AvroTypeException
        {
            requireNonNull(mapReadAction, "mapReadAction is null");
            valueBlockBuildingDecoder = typeManager.blockBuildingDecoderFor(mapReadAction.valueReadAction());
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
                long entriesInBlock = decoder.readMapStart();
                // TODO need to filter out all but last value for key?
                if (entriesInBlock > 0) {
                    do {
                        for (int i = 0; i < entriesInBlock; i++) {
                            keyBlockBuildingDecoder.decodeIntoBlock(decoder, keyBuilder);
                            valueBlockBuildingDecoder.decodeIntoBlock(decoder, valueBuilder);
                        }
                    }
                    while ((entriesInBlock = decoder.mapNext()) > 0);
                }
            });
        }
    }

    static class WriterUnionBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        protected final BlockBuildingDecoder[] blockBuildingDecoders;

        public WriterUnionBlockBuildingDecoder(WrittenUnionReadAction writtenUnionReadAction, AvroTypeBlockHandler typeManager)
                throws AvroTypeException
        {
            requireNonNull(writtenUnionReadAction, "writerUnion is null");
            blockBuildingDecoders = new BlockBuildingDecoder[writtenUnionReadAction.writeOptionReadActions().size()];
            for (int i = 0; i < writtenUnionReadAction.writeOptionReadActions().size(); i++) {
                blockBuildingDecoders[i] = typeManager.blockBuildingDecoderFor(writtenUnionReadAction.writeOptionReadActions().get(i));
            }
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decodeIntoBlock(decoder.readIndex(), decoder, builder);
        }

        protected void decodeIntoBlock(int blockBuilderIndex, Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            blockBuildingDecoders[blockBuilderIndex].decodeIntoBlock(decoder, builder);
        }
    }
}
