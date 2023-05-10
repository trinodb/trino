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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
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
import java.util.Arrays;
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

    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(Resolver.Action action, AvroTypeManager typeManager)
            throws AvroTypeException
    {
        Optional<BiConsumer<BlockBuilder, Object>> consumer = typeManager.overrideBuildingFunctionForSchema(action.reader);
        if (consumer.isPresent()) {
            return new UserDefinedBlockBuildingDecoder(action.reader, action.writer, consumer.get());
        }
        return switch (action.type) {
            case DO_NOTHING -> switch (action.reader.getType()) {
                case NULL -> NullBlockBuildingDecoder.INSTANCE;
                case BOOLEAN -> BooleanBlockBuildingDecoder.INSTANCE;
                case INT -> IntBlockBuildingDecoder.INSTANCE;
                case LONG -> new LongBlockBuildingDecoder();
                case FLOAT -> new FloatBlockBuildingDecoder();
                case DOUBLE -> new DoubleBlockBuildingDecoder();
                case STRING -> StringBlockBuildingDecoder.INSTANCE;
                case BYTES -> BytesBlockBuildingDecoder.INSTANCE;
                case FIXED -> new FixedBlockBuildingDecoder(action.reader.getFixedSize());
                // these reader types covered by special action types
                case ENUM, ARRAY, MAP, RECORD, UNION -> throw new IllegalStateException("Do Nothing action type not compatible with reader schema type " + action.reader.getType());
            };
            case PROMOTE -> switch (action.reader.getType()) {
                // only certain types valid to promote into as determined by org.apache.avro.Resolver.Promote.isValid
                case LONG -> new LongBlockBuildingDecoder(getLongPromotionFunction(action.writer));
                case FLOAT -> new FloatBlockBuildingDecoder(getFloatPromotionFunction(action.writer));
                case DOUBLE -> new DoubleBlockBuildingDecoder(getDoublePromotionFunction(action.writer));
                case STRING -> {
                    if (action.writer.getType() == Schema.Type.BYTES) {
                        yield StringBlockBuildingDecoder.INSTANCE;
                    }
                    throw new AvroTypeException("Unable to promote to String from type " + action.writer.getType());
                }
                case BYTES -> {
                    if (action.writer.getType() == Schema.Type.STRING) {
                        yield BytesBlockBuildingDecoder.INSTANCE;
                    }
                    throw new AvroTypeException("Unable to promote to Bytes from type " + action.writer.getType());
                }
                case NULL, BOOLEAN, INT, FIXED, ENUM, ARRAY, MAP, RECORD, UNION ->
                        throw new AvroTypeException("Promotion action not allowed for reader schema type " + action.reader.getType());
            };
            case CONTAINER -> switch (action.reader.getType()) {
                case ARRAY -> new ArrayBlockBuildingDecoder((Resolver.Container) action, typeManager);
                case MAP -> new MapBlockBuildingDecoder((Resolver.Container) action, typeManager);
                default -> throw new AvroTypeException("Not possible to have container action type with non container reader schema " + action.reader.getType());
            };
            case RECORD -> new RowBlockBuildingDecoder(action, typeManager);
            case ENUM -> new EnumBlockBuildingDecoder((Resolver.EnumAdjust) action);
            case WRITER_UNION -> {
                if (isSimpleNullableUnion(action.reader)) {
                    yield new WriterUnionBlockBuildingDecoder((Resolver.WriterUnion) action, typeManager);
                }
                else {
                    yield new WriterUnionCoercedIntoRowBlockBuildingDecoder((Resolver.WriterUnion) action, typeManager);
                }
            }
            case READER_UNION -> {
                if (isSimpleNullableUnion(action.reader)) {
                    yield createBlockBuildingDecoderForAction(((Resolver.ReaderUnion) action).actualAction, typeManager);
                }
                else {
                    yield new ReaderUnionCoercedIntoRowBlockBuildingDecoder((Resolver.ReaderUnion) action, typeManager);
                }
            }
            case ERROR -> throw new AvroTypeException("Resolution action returned with error " + action);
            case SKIP -> throw new IllegalStateException("Skips filtered by row step");
        };
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
        private static final LongIoFunction<Decoder> DEFAULT_EXTRACT_LONG = Decoder::readLong;
        private final LongIoFunction<Decoder> extractLong;

        public LongBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_LONG);
        }

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
        private static final FloatIoFunction<Decoder> DEFAULT_EXTRACT_FLOAT = Decoder::readFloat;
        private final FloatIoFunction<Decoder> extractFloat;

        public FloatBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_FLOAT);
        }

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
        private static final DoubleIoFunction<Decoder> DEFAULT_EXTRACT_DOUBLE = Decoder::readDouble;
        private final DoubleIoFunction<Decoder> extractDouble;

        public DoubleBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_DOUBLE);
        }

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
        private Slice[] symbols;

        public EnumBlockBuildingDecoder(Resolver.EnumAdjust action)
                throws AvroTypeException
        {
            List<String> symbolsList = requireNonNull(action, "action is null").reader.getEnumSymbols();
            symbols = symbolsList.stream().map(Slices::utf8Slice).toArray(Slice[]::new);
            if (!action.noAdjustmentsNeeded) {
                Slice[] adjustedSymbols = new Slice[(action.writer.getEnumSymbols().size())];
                for (int i = 0; i < action.adjustments.length; i++) {
                    if (action.adjustments[i] < 0) {
                        throw new AvroTypeException("No reader Enum value for writer Enum value " + action.writer.getEnumSymbols().get(i));
                    }
                    adjustedSymbols[i] = symbols[action.adjustments[i]];
                }
                symbols = adjustedSymbols;
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeSlice(builder, symbols[decoder.readEnum()]);
        }
    }

    private static class ArrayBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(Resolver.Container containerAction, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.ARRAY, "Reader schema must be a array");
            elementBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction, typeManager);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BlockBuilder elementBuilder = builder.beginBlockEntry();
            long elementsInBlock = decoder.readArrayStart();
            if (elementsInBlock > 0) {
                do {
                    for (int i = 0; i < elementsInBlock; i++) {
                        elementBlockBuildingDecoder.decodeIntoBlock(decoder, elementBuilder);
                    }
                }
                while ((elementsInBlock = decoder.arrayNext()) > 0);
            }
            builder.closeEntry();
        }
    }

    private static class MapBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder keyBlockBuildingDecoder = new StringBlockBuildingDecoder();
        private final BlockBuildingDecoder valueBlockBuildingDecoder;

        public MapBlockBuildingDecoder(Resolver.Container containerAction, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.MAP, "Reader schema must be a map");
            valueBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction, typeManager);
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BlockBuilder entryBuilder = builder.beginBlockEntry();
            long entriesInBlock = decoder.readMapStart();
            // TODO need to filter out all but last value for key?
            if (entriesInBlock > 0) {
                do {
                    for (int i = 0; i < entriesInBlock; i++) {
                        keyBlockBuildingDecoder.decodeIntoBlock(decoder, entryBuilder);
                        valueBlockBuildingDecoder.decodeIntoBlock(decoder, entryBuilder);
                    }
                }
                while ((entriesInBlock = decoder.mapNext()) > 0);
            }
            builder.closeEntry();
        }
    }

    private static class RowBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final RowBuildingAction[] buildSteps;

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            this(Resolver.resolve(writeSchema, readSchema, new GenericData()), typeManager);
        }

        private RowBlockBuildingDecoder(Resolver.Action action, AvroTypeManager typeManager)
                throws AvroTypeException

        {
            if (action instanceof Resolver.ErrorAction errorAction) {
                throw new AvroTypeException("Error in resolution of types for row building: " + errorAction.error);
            }
            if (!(action instanceof Resolver.RecordAdjust recordAdjust)) {
                throw new AvroTypeException("Write and Read Schemas must be records when building a row block building decoder. Illegal action: " + action);
            }
            buildSteps = new RowBuildingAction[recordAdjust.fieldActions.length + recordAdjust.readerOrder.length
                    - recordAdjust.firstDefault];
            int i = 0;
            int readerFieldCount = 0;
            for (; i < recordAdjust.fieldActions.length; i++) {
                Resolver.Action fieldAction = recordAdjust.fieldActions[i];
                if (fieldAction instanceof Resolver.Skip skip) {
                    buildSteps[i] = new SkipSchemaBuildingAction(skip.writer);
                }
                else {
                    Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                    buildSteps[i] = new BuildIntoBlockAction(createBlockBuildingDecoderForAction(fieldAction, typeManager), readField.pos());
                }
            }

            // add defaulting if required
            for (; i < buildSteps.length; i++) {
                // create constant block
                Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                // TODO see if it can be done with RLE block
                buildSteps[i] = new ConstantBlockAction(getDefaultBlockBuilder(readField, typeManager), readField.pos());
            }

            verify(Arrays.stream(buildSteps)
                            .mapToInt(RowBuildingAction::getOutputChannel)
                            .filter(a -> a >= 0)
                            .distinct()
                            .sum() == (recordAdjust.reader.getFields().size() * (recordAdjust.reader.getFields().size() - 1) / 2),
                    "Every channel in output block builder must be accounted for");
            verify(Arrays.stream(buildSteps)
                    .mapToInt(RowBuildingAction::getOutputChannel)
                    .filter(a -> a >= 0)
                    .distinct().count() == (long) recordAdjust.reader.getFields().size(), "Every channel in output block builder must be accounted for");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            SingleRowBlockWriter currentBuilder = (SingleRowBlockWriter) builder.beginBlockEntry();
            decodeIntoBlockProvided(decoder, currentBuilder::getFieldBlockBuilder);
            builder.closeEntry();
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
                // TODO replace with switch sealed class syntax when stable
                if (buildStep instanceof SkipSchemaBuildingAction skipSchemaBuildingAction) {
                    skipSchemaBuildingAction.skip(decoder);
                }
                else if (buildStep instanceof BuildIntoBlockAction buildIntoBlockAction) {
                    buildIntoBlockAction.decode(decoder, fieldBlockBuilder);
                }
                else if (buildStep instanceof ConstantBlockAction constantBlockAction) {
                    constantBlockAction.addConstant(fieldBlockBuilder);
                }
                else {
                    throw new IllegalStateException("Unhandled buildingAction");
                }
            }
        }

        sealed interface RowBuildingAction
                permits BuildIntoBlockAction, ConstantBlockAction, SkipSchemaBuildingAction
        {
            int getOutputChannel();
        }

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

            @Override
            public int getOutputChannel()
            {
                return outputChannel;
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

            @Override
            public int getOutputChannel()
            {
                return outputChannel;
            }
        }

        private static final class SkipSchemaBuildingAction
                implements RowBuildingAction
        {
            private final SkipAction skipAction;

            SkipSchemaBuildingAction(Schema schema)
            {
                skipAction = createSkipActionForSchema(requireNonNull(schema, "schema is null"));
            }

            public void skip(Decoder decoder)
                    throws IOException
            {
                skipAction.skip(decoder);
            }

            @Override
            public int getOutputChannel()
            {
                return -1;
            }

            @FunctionalInterface
            private interface SkipAction
            {
                void skip(Decoder decoder)
                        throws IOException;
            }

            private static SkipAction createSkipActionForSchema(Schema schema)
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

        public WriterUnionBlockBuildingDecoder(Resolver.WriterUnion writerUnion, AvroTypeManager typeManager)
                throws AvroTypeException
        {
            requireNonNull(writerUnion, "writerUnion is null");
            blockBuildingDecoders = new BlockBuildingDecoder[writerUnion.actions.length];
            for (int i = 0; i < writerUnion.actions.length; i++) {
                blockBuildingDecoders[i] = createBlockBuildingDecoderForAction(writerUnion.actions[i], typeManager);
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

        public WriterUnionCoercedIntoRowBlockBuildingDecoder(Resolver.WriterUnion writerUnion, AvroTypeManager avroTypeManager)
                throws AvroTypeException
        {
            super(writerUnion, avroTypeManager);
            readUnionEquiv = writerUnion.unionEquiv;
            List<Schema> readSchemas = writerUnion.reader.getTypes();
            checkArgument(readSchemas.size() == writerUnion.actions.length, "each read schema must have resolvedAction For it");
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
            SingleRowBlockWriter currentBuilder = (SingleRowBlockWriter) builder.beginBlockEntry();
            //add tag with channel
            UNION_FIELD_TAG_TYPE.writeLong(currentBuilder.getFieldBlockBuilder(0), outputChannel);
            //add in null fields except one
            for (int channel = 1; channel <= totalChannels; channel++) {
                if (channel == outputChannel + 1) {
                    blockBuildingDecoder.decodeIntoBlock(decoder, currentBuilder.getFieldBlockBuilder(channel));
                }
                else {
                    currentBuilder.getFieldBlockBuilder(channel).appendNull();
                }
            }
            builder.closeEntry();
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

        public ReaderUnionCoercedIntoRowBlockBuildingDecoder(Resolver.ReaderUnion readerUnion, AvroTypeManager avroTypeManager)
                throws AvroTypeException
        {
            requireNonNull(readerUnion, "readerUnion is null");
            requireNonNull(avroTypeManager, "avroTypeManger is null");
            int[] indexToChannel = WriterUnionCoercedIntoRowBlockBuildingDecoder.getIndexToChannel(readerUnion.reader.getTypes());
            outputChannel = indexToChannel[readerUnion.firstMatch];
            delegateBuilder = createBlockBuildingDecoderForAction(readerUnion.actualAction, avroTypeManager);
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

    private static LongIoFunction<Decoder> getLongPromotionFunction(Schema writerSchema)
            throws AvroTypeException
    {
        if (writerSchema.getType() == Schema.Type.INT) {
            return Decoder::readInt;
        }
        throw new AvroTypeException("Cannot promote type %s to long".formatted(writerSchema.getType()));
    }

    private static FloatIoFunction<Decoder> getFloatPromotionFunction(Schema writerSchema)
            throws AvroTypeException
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            default -> throw new AvroTypeException("Cannot promote type %s to float".formatted(writerSchema.getType()));
        };
    }

    private static DoubleIoFunction<Decoder> getDoublePromotionFunction(Schema writerSchema)
            throws AvroTypeException
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            case FLOAT -> Decoder::readFloat;
            default -> throw new AvroTypeException("Cannot promote type %s to double".formatted(writerSchema.getType()));
        };
    }

    @FunctionalInterface
    private interface LongIoFunction<A>
    {
        long apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    private interface FloatIoFunction<A>
    {
        float apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    private interface DoubleIoFunction<A>
    {
        double apply(A a)
                throws IOException;
    }

    @FunctionalInterface
    private interface IoConsumer<A>
    {
        void accept(A a)
                throws IOException;
    }

    // Avro supports default values for reader record fields that are missing in the writer schema
    // the bytes representing the default field value are passed to a block building decoder
    // so that it can pack the block appropriately for the default type.
    private static IoConsumer<BlockBuilder> getDefaultBlockBuilder(Schema.Field field, AvroTypeManager typeManager)
            throws AvroTypeException
    {
        BlockBuildingDecoder buildingDecoder = createBlockBuildingDecoderForAction(Resolver.resolve(field.schema(), field.schema()), typeManager);
        byte[] defaultBytes = getDefaultByes(field);
        BinaryDecoder reuse = DecoderFactory.get().binaryDecoder(defaultBytes, null);
        return blockBuilder -> buildingDecoder.decodeIntoBlock(DecoderFactory.get().binaryDecoder(defaultBytes, reuse), blockBuilder);
    }

    private static byte[] getDefaultByes(Schema.Field field)
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
