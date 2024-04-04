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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.model.ArrayReadAction;
import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.hive.formats.avro.model.BooleanRead;
import io.trino.hive.formats.avro.model.BytesRead;
import io.trino.hive.formats.avro.model.DefaultValueFieldRecordFieldReadAction;
import io.trino.hive.formats.avro.model.DoubleRead;
import io.trino.hive.formats.avro.model.EnumReadAction;
import io.trino.hive.formats.avro.model.FixedRead;
import io.trino.hive.formats.avro.model.FloatRead;
import io.trino.hive.formats.avro.model.IntRead;
import io.trino.hive.formats.avro.model.LongRead;
import io.trino.hive.formats.avro.model.MapReadAction;
import io.trino.hive.formats.avro.model.NullRead;
import io.trino.hive.formats.avro.model.ReadErrorReadAction;
import io.trino.hive.formats.avro.model.ReadFieldAction;
import io.trino.hive.formats.avro.model.ReadingUnionReadAction;
import io.trino.hive.formats.avro.model.RecordFieldReadAction;
import io.trino.hive.formats.avro.model.RecordReadAction;
import io.trino.hive.formats.avro.model.SkipFieldRecordFieldReadAction;
import io.trino.hive.formats.avro.model.StringRead;
import io.trino.hive.formats.avro.model.WrittenUnionReadAction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.FastReaderBuilder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.util.internal.Accessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.UnionToRowCoercionUtils.UNION_FIELD_TAG_TYPE;
import static io.trino.hive.formats.avro.AvroTypeUtils.isSimpleNullableUnion;
import static io.trino.hive.formats.avro.AvroTypeUtils.typeFromAvro;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class AvroPageDataReader
        implements DatumReader<Optional<Page>>
{
    // same limit as org.apache.avro.io.BinaryDecoder
    private static final long MAX_ARRAY_SIZE = (long) Integer.MAX_VALUE - 8L;

    private final Schema readerSchema;
    private Schema writerSchema;
    private final PageBuilder pageBuilder;
    private RowBlockBuildingDecoder rowBlockBuildingDecoder;
    private final AvroTypeManager typeManager;

    public AvroPageDataReader(Schema readerSchema, AvroTypeManager typeManager)
            throws AvroTypeException
    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        writerSchema = this.readerSchema;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        try {
            Type readerSchemaType = typeFromAvro(this.readerSchema, typeManager);
            verify(readerSchemaType instanceof RowType, "Root Avro type must be a row");
            pageBuilder = new PageBuilder(readerSchemaType.getTypeParameters());
            initialize();
        }
        catch (org.apache.avro.AvroTypeException e) {
            throw new AvroTypeException(e);
        }
    }

    private void initialize()
            throws AvroTypeException
    {
        verify(readerSchema.getType() == Schema.Type.RECORD, "Avro schema for page reader must be record");
        verify(writerSchema.getType() == Schema.Type.RECORD, "File Avro schema for page reader must be record");
        rowBlockBuildingDecoder = new RowBlockBuildingDecoder(writerSchema, readerSchema, typeManager);
    }

    @Override
    public void setSchema(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        if (schema != writerSchema) {
            writerSchema = schema;
            try {
                initialize();
            }
            catch (org.apache.avro.AvroTypeException e) {
                throw new UncheckedAvroTypeException(new AvroTypeException(e));
            }
            catch (AvroTypeException e) {
                throw new UncheckedAvroTypeException(e);
            }
        }
    }

    @Override
    public Optional<Page> read(Optional<Page> ignoredReuse, Decoder decoder)
            throws IOException
    {
        Optional<Page> page = Optional.empty();
        rowBlockBuildingDecoder.decodeIntoPageBuilder(decoder, pageBuilder);
        if (pageBuilder.isFull()) {
            page = Optional.of(pageBuilder.build());
            pageBuilder.reset();
        }
        return page;
    }

    public Optional<Page> flush()
    {
        if (!pageBuilder.isEmpty()) {
            Optional<Page> lastPage = Optional.of(pageBuilder.build());
            pageBuilder.reset();
            return lastPage;
        }
        return Optional.empty();
    }

    private abstract static class BlockBuildingDecoder
    {
        protected abstract void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException;
    }

    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(AvroReadAction action, AvroTypeManager typeManager)
            throws AvroTypeException
    {
        Optional<BiConsumer<BlockBuilder, Object>> consumer = typeManager.overrideBuildingFunctionForSchema(action.readSchema());
        if (consumer.isPresent()) {
            return new UserDefinedBlockBuildingDecoder(action.readSchema(), action.writeSchema(), consumer.get());
        }
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
            case ArrayReadAction arrayReadAction -> new ArrayBlockBuildingDecoder(arrayReadAction, typeManager);
            case MapReadAction mapReadAction -> new MapBlockBuildingDecoder(mapReadAction, typeManager);
            case EnumReadAction enumReadAction -> new EnumBlockBuildingDecoder(enumReadAction);
            case RecordReadAction recordReadAction -> new RowBlockBuildingDecoder(recordReadAction, typeManager);
            case WrittenUnionReadAction writtenUnionReadAction -> {
                if (writtenUnionReadAction.readSchema().getType() == Schema.Type.UNION && !isSimpleNullableUnion(writtenUnionReadAction.readSchema())) {
                    yield new WriterUnionCoercedIntoRowBlockBuildingDecoder(writtenUnionReadAction, typeManager);
                }
                else {
                    // reading a union with non-union or nullable union, optimistically try to create the reader, will fail at read time with any underlying issues
                    yield new WriterUnionBlockBuildingDecoder(writtenUnionReadAction, typeManager);
                }
            }
            case ReadingUnionReadAction readingUnionReadAction -> {
                if (isSimpleNullableUnion(readingUnionReadAction.readSchema())) {
                    yield createBlockBuildingDecoderForAction(readingUnionReadAction.actualAction(), typeManager);
                }
                else {
                    yield new ReaderUnionCoercedIntoRowBlockBuildingDecoder(readingUnionReadAction, typeManager);
                }
            }
            case ReadErrorReadAction readErrorReadAction -> new TypeErrorThrower(readErrorReadAction);
        };
    }

    private static class TypeErrorThrower
            extends BlockBuildingDecoder
    {
        private final ReadErrorReadAction action;

        public TypeErrorThrower(ReadErrorReadAction action)
        {
            this.action = requireNonNull(action, "action is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            throw new IOException(new AvroTypeException("Resolution action returned with error " + action));
        }
    }

    // Different plugins may have different Avro Schema to Type mappings
    // that are currently transforming GenericDatumReader returned objects into their target type during the record reading process
    // This block building decoder allows plugin writers to port that code directly and use within this reader
    // This mechanism is used to enhance Avro longs into timestamp types according to schema metadata
    private static class UserDefinedBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BiConsumer<BlockBuilder, Object> userBuilderFunction;
        private final DatumReader<Object> datumReader;

        public UserDefinedBlockBuildingDecoder(Schema readerSchema, Schema writerSchema, BiConsumer<BlockBuilder, Object> userBuilderFunction)
                throws AvroTypeException
        {
            requireNonNull(readerSchema, "readerSchema is null");
            requireNonNull(writerSchema, "writerSchema is null");
            try {
                FastReaderBuilder fastReaderBuilder = new FastReaderBuilder(new GenericData());
                datumReader = fastReaderBuilder.createDatumReader(writerSchema, readerSchema);
            }
            catch (IOException ioException) {
                // IOException only thrown when default encoded in schema is unable to be re-serialized into bytes with proper typing
                // translate into type exception
                throw new AvroTypeException("Unable to decode default value in schema " + readerSchema, ioException);
            }
            this.userBuilderFunction = requireNonNull(userBuilderFunction, "userBuilderFunction is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            userBuilderFunction.accept(builder, datumReader.read(null, decoder));
        }
    }

    private static class NullBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final NullBlockBuildingDecoder INSTANCE = new NullBlockBuildingDecoder();

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readNull();
            builder.appendNull();
        }
    }

    private static class BooleanBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final BooleanBlockBuildingDecoder INSTANCE = new BooleanBlockBuildingDecoder();

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BOOLEAN.writeBoolean(builder, decoder.readBoolean());
        }
    }

    private static class IntBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final IntBlockBuildingDecoder INSTANCE = new IntBlockBuildingDecoder();

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, decoder.readInt());
        }
    }

    private static class LongBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final LongIoFunction<Decoder> extractLong;

        public LongBlockBuildingDecoder(LongIoFunction<Decoder> extractLong)
        {
            this.extractLong = requireNonNull(extractLong, "extractLong is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BIGINT.writeLong(builder, extractLong.apply(decoder));
        }
    }

    private static class FloatBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final FloatIoFunction<Decoder> extractFloat;

        public FloatBlockBuildingDecoder(FloatIoFunction<Decoder> extractFloat)
        {
            this.extractFloat = requireNonNull(extractFloat, "extractFloat is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            REAL.writeLong(builder, floatToRawIntBits(extractFloat.apply(decoder)));
        }
    }

    private static class DoubleBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final DoubleIoFunction<Decoder> extractDouble;

        public DoubleBlockBuildingDecoder(DoubleIoFunction<Decoder> extractDouble)
        {
            this.extractDouble = requireNonNull(extractDouble, "extractDouble is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            DOUBLE.writeDouble(builder, extractDouble.apply(decoder));
        }
    }

    private static class StringBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final StringBlockBuildingDecoder INSTANCE = new StringBlockBuildingDecoder();

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
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

    private static class BytesBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final BytesBlockBuildingDecoder INSTANCE = new BytesBlockBuildingDecoder();

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
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

    private static class FixedBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final int expectedSize;

        public FixedBlockBuildingDecoder(int expectedSize)
        {
            verify(expectedSize >= 0, "expected size must be greater than or equal to 0");
            this.expectedSize = expectedSize;
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            byte[] slice = new byte[expectedSize];
            decoder.readFixed(slice);
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer(slice));
        }
    }

    private static class EnumBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final List<Slice> symbols;

        public EnumBlockBuildingDecoder(EnumReadAction enumReadAction)
        {
            requireNonNull(enumReadAction, "action is null");
            symbols = enumReadAction.getSymbolIndex();
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeSlice(builder, symbols.get(decoder.readEnum()));
        }
    }

    private static class ArrayBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(ArrayReadAction arrayReadAction, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(arrayReadAction, "arrayReadAction is null");
            elementBlockBuildingDecoder = createBlockBuildingDecoderForAction(arrayReadAction.elementReadAction(), typeManager);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
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

    private static class MapBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder keyBlockBuildingDecoder = new StringBlockBuildingDecoder();
        private final BlockBuildingDecoder valueBlockBuildingDecoder;

        public MapBlockBuildingDecoder(MapReadAction mapReadAction, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(mapReadAction, "mapReadAction is null");
            valueBlockBuildingDecoder = createBlockBuildingDecoderForAction(mapReadAction.valueReadAction(), typeManager);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
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

    public static class RowBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final RowBuildingAction[] buildSteps;

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            this(AvroReadAction.fromAction(Resolver.resolve(writeSchema, readSchema, new GenericData())), typeManager);
        }

        private RowBlockBuildingDecoder(AvroReadAction action, AvroTypeManager typeManager)
                throws AvroTypeException

        {
            if (!(action instanceof RecordReadAction recordReadAction)) {
                throw new AvroTypeException("Write and Read Schemas must be records when building a row block building decoder. Illegal action: " + action);
            }
            buildSteps = new RowBuildingAction[recordReadAction.fieldReadActions().size()];
            int i = 0;
            for (RecordFieldReadAction fieldAction : recordReadAction.fieldReadActions()) {
                buildSteps[i] = switch (fieldAction) {
                    case DefaultValueFieldRecordFieldReadAction defaultValueFieldRecordFieldReadAction -> new ConstantBlockAction(
                            getDefaultBlockBuilder(defaultValueFieldRecordFieldReadAction.fieldSchema(),
                                    defaultValueFieldRecordFieldReadAction.defaultBytes(), typeManager),
                            defaultValueFieldRecordFieldReadAction.outputChannel());
                    case ReadFieldAction readFieldAction -> new BuildIntoBlockAction(createBlockBuildingDecoderForAction(readFieldAction.readAction(), typeManager),
                            readFieldAction.outputChannel());
                    case SkipFieldRecordFieldReadAction skipFieldRecordFieldReadAction -> new SkipSchemaBuildingAction(skipFieldRecordFieldReadAction.skipAction());
                };
                i++;
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> decodeIntoBlockProvided(decoder, fieldBuilders::get));
        }

        protected void decodeIntoPageBuilder(Decoder decoder, PageBuilder builder)
                throws IOException
        {
            builder.declarePosition();
            decodeIntoBlockProvided(decoder, builder::getBlockBuilder);
        }

        protected void decodeIntoBlockProvided(Decoder decoder, IntFunction<BlockBuilder> fieldBlockBuilder)
                throws IOException
        {
            for (RowBuildingAction buildStep : buildSteps) {
                switch (buildStep) {
                    case SkipSchemaBuildingAction skipSchemaBuildingAction -> skipSchemaBuildingAction.skip(decoder);
                    case BuildIntoBlockAction buildIntoBlockAction -> buildIntoBlockAction.decode(decoder, fieldBlockBuilder);
                    case ConstantBlockAction constantBlockAction -> constantBlockAction.addConstant(fieldBlockBuilder);
                }
            }
        }

        sealed interface RowBuildingAction
                permits BuildIntoBlockAction, ConstantBlockAction, SkipSchemaBuildingAction
        {}

        private static final class BuildIntoBlockAction
                implements RowBuildingAction
        {
            private final BlockBuildingDecoder delegate;
            private final int outputChannel;

            public BuildIntoBlockAction(BlockBuildingDecoder delegate, int outputChannel)
            {
                this.delegate = requireNonNull(delegate, "delegate is null");
                checkArgument(outputChannel >= 0, "outputChannel must be positive");
                this.outputChannel = outputChannel;
            }

            public void decode(Decoder decoder, IntFunction<BlockBuilder> channelSelector)
                    throws IOException
            {
                delegate.decodeIntoBlock(decoder, channelSelector.apply(outputChannel));
            }
        }

        protected static final class ConstantBlockAction
                implements RowBuildingAction
        {
            private final IoConsumer<BlockBuilder> addConstantFunction;
            private final int outputChannel;

            public ConstantBlockAction(IoConsumer<BlockBuilder> addConstantFunction, int outputChannel)
            {
                this.addConstantFunction = requireNonNull(addConstantFunction, "addConstantFunction is null");
                checkArgument(outputChannel >= 0, "outputChannel must be positive");
                this.outputChannel = outputChannel;
            }

            public void addConstant(IntFunction<BlockBuilder> channelSelector)
                    throws IOException
            {
                addConstantFunction.accept(channelSelector.apply(outputChannel));
            }
        }

        public static final class SkipSchemaBuildingAction
                implements RowBuildingAction
        {
            private final SkipAction skipAction;

            SkipSchemaBuildingAction(SkipAction skipAction)
            {
                this.skipAction = requireNonNull(skipAction, "skipAction is null");
            }

            public void skip(Decoder decoder)
                    throws IOException
            {
                skipAction.skip(decoder);
            }

            @FunctionalInterface
            public interface SkipAction
            {
                void skip(Decoder decoder)
                        throws IOException;
            }

            public static SkipAction createSkipActionForSchema(Schema schema)
            {
                return switch (schema.getType()) {
                    case NULL -> Decoder::readNull;
                    case BOOLEAN -> Decoder::readBoolean;
                    case INT -> Decoder::readInt;
                    case LONG -> Decoder::readLong;
                    case FLOAT -> Decoder::readFloat;
                    case DOUBLE -> Decoder::readDouble;
                    case STRING -> Decoder::skipString;
                    case BYTES -> Decoder::skipBytes;
                    case ENUM -> Decoder::readEnum;
                    case FIXED -> {
                        int size = schema.getFixedSize();
                        yield decoder -> decoder.skipFixed(size);
                    }
                    case ARRAY -> new ArraySkipAction(schema.getElementType());
                    case MAP -> new MapSkipAction(schema.getValueType());
                    case RECORD -> new RecordSkipAction(schema.getFields());
                    case UNION -> new UnionSkipAction(schema.getTypes());
                };
            }

            private static class ArraySkipAction
                    implements SkipAction
            {
                private final SkipAction elementSkipAction;

                public ArraySkipAction(Schema elementSchema)
                {
                    elementSkipAction = createSkipActionForSchema(requireNonNull(elementSchema, "elementSchema is null"));
                }

                @Override
                public void skip(Decoder decoder)
                        throws IOException
                {
                    for (long i = decoder.skipArray(); i != 0; i = decoder.skipArray()) {
                        for (long j = 0; j < i; j++) {
                            elementSkipAction.skip(decoder);
                        }
                    }
                }
            }

            private static class MapSkipAction
                    implements SkipAction
            {
                private final SkipAction valueSkipAction;

                public MapSkipAction(Schema valueSchema)
                {
                    valueSkipAction = createSkipActionForSchema(requireNonNull(valueSchema, "valueSchema is null"));
                }

                @Override
                public void skip(Decoder decoder)
                        throws IOException
                {
                    for (long i = decoder.skipMap(); i != 0; i = decoder.skipMap()) {
                        for (long j = 0; j < i; j++) {
                            decoder.skipString(); // key
                            valueSkipAction.skip(decoder); // value
                        }
                    }
                }
            }

            private static class RecordSkipAction
                    implements SkipAction
            {
                private final SkipAction[] fieldSkips;

                public RecordSkipAction(List<Schema.Field> fields)
                {
                    fieldSkips = new SkipAction[requireNonNull(fields, "fields is null").size()];
                    for (int i = 0; i < fields.size(); i++) {
                        fieldSkips[i] = createSkipActionForSchema(fields.get(i).schema());
                    }
                }

                @Override
                public void skip(Decoder decoder)
                        throws IOException
                {
                    for (SkipAction fieldSkipAction : fieldSkips) {
                        fieldSkipAction.skip(decoder);
                    }
                }
            }

            private static class UnionSkipAction
                    implements SkipAction
            {
                private final SkipAction[] skipActions;

                private UnionSkipAction(List<Schema> types)
                {
                    skipActions = new SkipAction[requireNonNull(types, "types is null").size()];
                    for (int i = 0; i < types.size(); i++) {
                        skipActions[i] = createSkipActionForSchema(types.get(i));
                    }
                }

                @Override
                public void skip(Decoder decoder)
                        throws IOException
                {
                    skipActions[decoder.readIndex()].skip(decoder);
                }
            }
        }
    }

    private static class WriterUnionBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        protected final BlockBuildingDecoder[] blockBuildingDecoders;

        public WriterUnionBlockBuildingDecoder(WrittenUnionReadAction writtenUnionReadAction, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(writtenUnionReadAction, "writerUnion is null");
            blockBuildingDecoders = new BlockBuildingDecoder[writtenUnionReadAction.writeOptionReadActions().size()];
            for (int i = 0; i < writtenUnionReadAction.writeOptionReadActions().size(); i++) {
                blockBuildingDecoders[i] = createBlockBuildingDecoderForAction(writtenUnionReadAction.writeOptionReadActions().get(i), typeManager);
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
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

    private static class WriterUnionCoercedIntoRowBlockBuildingDecoder
            extends WriterUnionBlockBuildingDecoder
    {
        private final boolean readUnionEquiv;
        private final int[] indexToChannel;
        private final int totalChannels;

        public WriterUnionCoercedIntoRowBlockBuildingDecoder(WrittenUnionReadAction writtenUnionReadAction, AvroTypeManager avroTypeManager)
                throws AvroTypeException
        {
            super(writtenUnionReadAction, avroTypeManager);
            readUnionEquiv = writtenUnionReadAction.unionEqiv();
            List<Schema> readSchemas = writtenUnionReadAction.readSchema().getTypes();
            checkArgument(readSchemas.size() == writtenUnionReadAction.writeOptionReadActions().size(), "each read schema must have resolvedAction For it");
            indexToChannel = getIndexToChannel(readSchemas);
            totalChannels = (int) IntStream.of(indexToChannel).filter(i -> i >= 0).count();
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            int index = decoder.readIndex();
            if (readUnionEquiv) {
                // if no output channel then the schema is null and the whole record can be null;
                if (indexToChannel[index] < 0) {
                    NullBlockBuildingDecoder.INSTANCE.decodeIntoBlock(decoder, builder);
                }
                else {
                    // the index for the reader and writer are the same, so the channel for the index is used to select the field to populate
                    makeSingleRowWithTagAndAllFieldsNullButOne(indexToChannel[index], totalChannels, blockBuildingDecoders[index], decoder, builder);
                }
            }
            else {
                // delegate to ReaderUnionCoercedIntoRowBlockBuildingDecoder to get the output channel from the resolved action
                decodeIntoBlock(index, decoder, builder);
            }
        }

        protected static void makeSingleRowWithTagAndAllFieldsNullButOne(int outputChannel, int totalChannels, BlockBuildingDecoder blockBuildingDecoder, Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
                //add tag with channel
                UNION_FIELD_TAG_TYPE.writeLong(fieldBuilders.getFirst(), outputChannel);
                //add in null fields except one
                for (int channel = 1; channel <= totalChannels; channel++) {
                    if (channel == outputChannel + 1) {
                        blockBuildingDecoder.decodeIntoBlock(decoder, fieldBuilders.get(channel));
                    }
                    else {
                        fieldBuilders.get(channel).appendNull();
                    }
                }
            });
        }

        protected static int[] getIndexToChannel(List<Schema> schemas)
        {
            int[] indexToChannel = new int[schemas.size()];
            int outputChannel = 0;
            for (int i = 0; i < indexToChannel.length; i++) {
                if (schemas.get(i).getType() == Schema.Type.NULL) {
                    indexToChannel[i] = -1;
                }
                else {
                    indexToChannel[i] = outputChannel++;
                }
            }
            return indexToChannel;
        }
    }

    private static class ReaderUnionCoercedIntoRowBlockBuildingDecoder
            extends
            BlockBuildingDecoder
    {
        private final BlockBuildingDecoder delegateBuilder;
        private final int outputChannel;
        private final int totalChannels;

        public ReaderUnionCoercedIntoRowBlockBuildingDecoder(ReadingUnionReadAction readingUnionReadAction, AvroTypeManager avroTypeManager)
                throws AvroTypeException
        {
            requireNonNull(readingUnionReadAction, "readerUnion is null");
            requireNonNull(avroTypeManager, "avroTypeManger is null");
            int[] indexToChannel = WriterUnionCoercedIntoRowBlockBuildingDecoder.getIndexToChannel(readingUnionReadAction.readSchema().getTypes());
            outputChannel = indexToChannel[readingUnionReadAction.firstMatch()];
            delegateBuilder = createBlockBuildingDecoderForAction(readingUnionReadAction.actualAction(), avroTypeManager);
            totalChannels = (int) IntStream.of(indexToChannel).filter(i -> i >= 0).count();
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            if (outputChannel < 0) {
                // No outputChannel for Null schema in union, null out coerces struct
                NullBlockBuildingDecoder.INSTANCE.decodeIntoBlock(decoder, builder);
            }
            else {
                WriterUnionCoercedIntoRowBlockBuildingDecoder
                        .makeSingleRowWithTagAndAllFieldsNullButOne(outputChannel, totalChannels, delegateBuilder, decoder, builder);
            }
        }
    }

    public static LongIoFunction<Decoder> getLongDecoderFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            default -> throw new IllegalArgumentException("Cannot promote type %s to long".formatted(writerSchema.getType()));
        };
    }

    public static FloatIoFunction<Decoder> getFloatDecoderFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            case FLOAT -> Decoder::readFloat;
            default -> throw new IllegalArgumentException("Cannot promote type %s to float".formatted(writerSchema.getType()));
        };
    }

    public static DoubleIoFunction<Decoder> getDoubleDecoderFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            case FLOAT -> Decoder::readFloat;
            case DOUBLE -> Decoder::readDouble;
            default -> throw new IllegalArgumentException("Cannot promote type %s to double".formatted(writerSchema.getType()));
        };
    }

    @FunctionalInterface
    public interface LongIoFunction<A>
    {
        long apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    public interface FloatIoFunction<A>
    {
        float apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    public interface DoubleIoFunction<A>
    {
        double apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    public interface IoConsumer<A>
    {
        void accept(A a)
                throws IOException;
    }

    // Avro supports default values for reader record fields that are missing in the writer schema
    // the bytes representing the default field value are passed to a block building decoder
    // so that it can pack the block appropriately for the default type.
    private static IoConsumer<BlockBuilder> getDefaultBlockBuilder(Schema fieldSchema, byte[] defaultBytes, AvroTypeManager typeManager)
            throws AvroTypeException
    {
        BlockBuildingDecoder buildingDecoder = createBlockBuildingDecoderForAction(AvroReadAction.fromAction(Resolver.resolve(fieldSchema, fieldSchema)), typeManager);
        BinaryDecoder reuse = DecoderFactory.get().binaryDecoder(defaultBytes, null);
        return blockBuilder -> buildingDecoder.decodeIntoBlock(DecoderFactory.get().binaryDecoder(defaultBytes, reuse), blockBuilder);
    }

    public static byte[] getDefaultByes(Schema.Field field)
            throws AvroTypeException
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder e = EncoderFactory.get().binaryEncoder(out, null);
            ResolvingGrammarGenerator.encode(e, field.schema(), Accessor.defaultValue(field));
            e.flush();
            return out.toByteArray();
        }
        catch (IOException exception) {
            throw new AvroTypeException("Unable to encode to bytes for default value in field " + field, exception);
        }
    }

    /**
     * Used for throwing {@link AvroTypeException} through interfaces that can not throw checked exceptions like DatumReader
     */
    protected static class UncheckedAvroTypeException
            extends RuntimeException
    {
        private final AvroTypeException avroTypeException;

        public UncheckedAvroTypeException(AvroTypeException cause)
        {
            super(requireNonNull(cause, "cause is null"));
            avroTypeException = cause;
        }

        public AvroTypeException getAvroTypeException()
        {
            return avroTypeException;
        }
    }
}
