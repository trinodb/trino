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

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.avro.AvroTypeException;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
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
    private final Schema readerSchema;
    private Schema writerSchema;
    private final PageBuilder pageBuilder;
    private RowBlockBuildingDecoder rowBlockBuildingDecoder;
    private final AvroTypeManager typeManager;

    public AvroPageDataReader(Schema readerSchema, AvroTypeManager typeManager)

    {
        this.readerSchema = requireNonNull(readerSchema, "readerSchema is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.writerSchema = this.readerSchema;
        Type readerSchemaType = typeFromAvro(this.readerSchema, typeManager);
        verify(readerSchemaType instanceof RowType, "Can only build pages when top level type is Row");
        this.pageBuilder = new PageBuilder(readerSchemaType.getTypeParameters());
        initialize();
    }

    private void initialize()
    {
        verify(readerSchema.getType().equals(Schema.Type.RECORD), "Avro schema for page reader must be record");
        verify(writerSchema.getType().equals(Schema.Type.RECORD), "File Avro schema for page reader must be record");
        this.rowBlockBuildingDecoder = new RowBlockBuildingDecoder(writerSchema, readerSchema, typeManager);
    }

    @Override
    public void setSchema(Schema schema)
    {
        if (schema != null && schema != writerSchema) {
            writerSchema = schema;
            initialize();
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

    private sealed interface RowBuildingAction
            permits SkipSchemaBuildingAction, BuildIntoBlockAction, ConstantBlockAction
    {
        int getOutputChannel();
    }

    @FunctionalInterface
    private interface SkipAction
    {
        void skip(Decoder decoder)
                throws IOException;
    }

    private static final class SkipSchemaBuildingAction
            implements RowBuildingAction, SkipAction
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
            case FIXED -> decoder -> decoder.skipFixed(schema.getFixedSize());
            case ARRAY -> new ArraySkipAction(schema.getElementType());
            case MAP -> new MapSkipAction(schema.getValueType());
            case UNION -> new UnionSkipAction(schema.getTypes());
            case RECORD -> new RecordSkipAction(schema.getFields());
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
            this.valueSkipAction = createSkipActionForSchema(requireNonNull(valueSchema, "valueSchema is null"));
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

    private static final class ConstantBlockAction
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

    private static class RowBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        RowBuildingAction[] buildSteps;

        private RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema, AvroTypeManager typeManager)
        {
            this(Resolver.resolve(writeSchema, readSchema, new GenericData()), typeManager);
        }

        private RowBlockBuildingDecoder(Resolver.Action action, AvroTypeManager typeManager)

        {
            if (action instanceof Resolver.ErrorAction errorAction) {
                throw new RuntimeException("Error in resolution of types for row building: " + errorAction.error);
            }
            else if (action instanceof Resolver.RecordAdjust recordAdjust) {
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
                    buildSteps[i] = new ConstantBlockAction(getDefaultBlockBuilder(readField, typeManager), readField.pos());
                }

                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(a -> a >= 0).distinct().sum() == (recordAdjust.reader.getFields().size() * (recordAdjust.reader.getFields().size() - 1) / 2),
                        "Every channel in output block builder must be accounted for");
                verify(Arrays.stream(buildSteps).mapToInt(RowBuildingAction::getOutputChannel).filter(a -> a >= 0).distinct().count() == (long) recordAdjust.reader.getFields().size(), "Every channel in output block builder must be accounted for");
            }
            else {
                throw new AvroTypeException("Write and Read Schemas must be records when building a row block building decoder");
            }
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

        protected void decodeIntoBlockProvided(Decoder decoder, IntFunction<BlockBuilder> fieldBuilders)
                throws IOException
        {
            for (int i = 0; i < buildSteps.length; i++) {
                // TODO replace with switch sealed class syntax when stable
                if (buildSteps[i] instanceof SkipSchemaBuildingAction skipSchemaBuildingAction) {
                    skipSchemaBuildingAction.skip(decoder);
                }
                else if (buildSteps[i] instanceof BuildIntoBlockAction buildIntoBlockAction) {
                    buildIntoBlockAction.decode(decoder, fieldBuilders);
                }
                else if (buildSteps[i] instanceof ConstantBlockAction constantBlockAction) {
                    constantBlockAction.addConstant(fieldBuilders);
                }
                else {
                    throw new IllegalStateException("Unhandled buildingAction");
                }
            }
        }
    }

    private static class MapBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder keyBlockBuildingDecoder = new StringBlockBuildingDecoder();
        private final BlockBuildingDecoder valueBlockBuildingDecoder;

        public MapBlockBuildingDecoder(Resolver.Container containerAction, AvroTypeManager typeManager)
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.MAP, "Reader schema must be a map");
            this.valueBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction, typeManager);
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

    private static class ArrayBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder elementBlockBuildingDecoder;

        public ArrayBlockBuildingDecoder(Resolver.Container containerAction, AvroTypeManager typeManager)
        {
            requireNonNull(containerAction, "containerAction is null");
            verify(containerAction.reader.getType() == Schema.Type.ARRAY, "Reader schema must be a array");
            this.elementBlockBuildingDecoder = createBlockBuildingDecoderForAction(containerAction.elementAction, typeManager);
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

    private static class IntBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
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

    private static class BooleanBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            BOOLEAN.writeBoolean(builder, decoder.readBoolean());
        }
    }

    private static class NullBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            decoder.readNull();
            builder.appendNull();
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

    private static class BytesBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final IOFunction<Decoder, Slice> DEFAULT_EXTRACT_BYTES = decoder -> Slices.wrappedBuffer(decoder.readBytes(ByteBuffer.allocate(32)));
        private final IOFunction<Decoder, Slice> extractBytes;

        public BytesBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_BYTES);
        }

        public BytesBlockBuildingDecoder(IOFunction<Decoder, Slice> extractBytes)
        {
            this.extractBytes = requireNonNull(extractBytes, "extractBytes is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARBINARY.writeSlice(builder, extractBytes.apply(decoder));
        }
    }

    private static class StringBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private static final IOFunction<Decoder, String> DEFAULT_EXTRACT_STRING = Decoder::readString;
        private final IOFunction<Decoder, String> extractString;

        public StringBlockBuildingDecoder()
        {
            this(DEFAULT_EXTRACT_STRING);
        }

        public StringBlockBuildingDecoder(IOFunction<Decoder, String> extractString)
        {
            this.extractString = requireNonNull(extractString, "extractString is null");
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            VARCHAR.writeString(builder, extractString.apply(decoder));
        }
    }

    private static class EnumBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private String[] symbols;

        public EnumBlockBuildingDecoder(Resolver.EnumAdjust action)
        {
            List<String> symbolsList = requireNonNull(action, "action is null").reader.getEnumSymbols();
            symbols = symbolsList.toArray(String[]::new);
            if (!action.noAdjustmentsNeeded) {
                String[] adjustedSymbols = new String[(action.writer.getEnumSymbols().size())];
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
            VARCHAR.writeString(builder, symbols[decoder.readEnum()]);
        }
    }

    private static class WriterUnionBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        private final BlockBuildingDecoder[] blockBuildingDecoders;

        public WriterUnionBlockBuildingDecoder(Resolver.WriterUnion writerUnion, AvroTypeManager typeManager)
        {
            blockBuildingDecoders = new BlockBuildingDecoder[writerUnion.actions.length];
            for (int i = 0; i < writerUnion.actions.length; i++) {
                blockBuildingDecoders[i] = createBlockBuildingDecoderForAction(writerUnion.actions[i], typeManager);
            }
        }

        @Override
        protected void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            int writeIndex = decoder.readIndex();
            blockBuildingDecoders[writeIndex].decodeIntoBlock(decoder, builder);
        }
    }

    private static class UserDefinedBlockBuildingDecoder
            extends BlockBuildingDecoder
    {
        FastReaderBuilder fastReaderBuilder = new FastReaderBuilder(new GenericData());
        BiConsumer<BlockBuilder, Object> userBuilderFunction;
        DatumReader<Object> datumReader;

        public UserDefinedBlockBuildingDecoder(Schema readerSchema, Schema writerSchema, BiConsumer<BlockBuilder, Object> userBuilderFunction)
        {
            requireNonNull(readerSchema, "readerSchema is null");
            requireNonNull(writerSchema, "writerSchema is null");
            try {
                this.datumReader = fastReaderBuilder.createDatumReader(writerSchema, readerSchema);
            }
            catch (IOException ioException) {
                throw new AvroTypeException("Unable to decode default value in schema " + readerSchema.toString(), ioException);
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

    private static BlockBuildingDecoder createBlockBuildingDecoderForAction(Resolver.Action action, AvroTypeManager typeManager)

    {
        Optional<BiConsumer<BlockBuilder, Object>> consumer = typeManager.overrideBuildingFunctionForSchema(action.reader);
        if (consumer.isPresent()) {
            return new UserDefinedBlockBuildingDecoder(action.reader, action.writer, consumer.get());
        }
        return switch (action.type) {
            case CONTAINER -> switch (action.reader.getType()) {
                case MAP -> new MapBlockBuildingDecoder((Resolver.Container) action, typeManager);
                case ARRAY -> new ArrayBlockBuildingDecoder((Resolver.Container) action, typeManager);
                default -> throw new AvroTypeException("Not possible to have container action type with non container reader schema " + action.reader.getType());
            };
            case DO_NOTHING -> switch (action.reader.getType()) {
                case INT -> new IntBlockBuildingDecoder();
                case LONG -> new LongBlockBuildingDecoder();
                case FLOAT -> new FloatBlockBuildingDecoder();
                case DOUBLE -> new DoubleBlockBuildingDecoder();
                case BOOLEAN -> new BooleanBlockBuildingDecoder();
                case NULL -> new NullBlockBuildingDecoder();
                case FIXED -> new FixedBlockBuildingDecoder(action.reader.getFixedSize());
                case STRING -> new StringBlockBuildingDecoder();
                case BYTES -> new BytesBlockBuildingDecoder();
                // these reader types covered by special action types
                case RECORD, ENUM, ARRAY, MAP, UNION -> {
                    throw new IllegalStateException("Do Nothing action type not compatible with reader schema type " + action.reader.getType());
                }
            };
            case RECORD -> new RowBlockBuildingDecoder(action, typeManager);
            case ENUM -> new EnumBlockBuildingDecoder((Resolver.EnumAdjust) action);
            case PROMOTE -> switch (action.reader.getType()) {
                // only certain types valid to promote into as determined by org.apache.avro.Resolver.Promote.isValid
                case BYTES -> new BytesBlockBuildingDecoder(getBytesPromotionFunction(action.writer));
                case STRING -> new StringBlockBuildingDecoder(getStringPromotionFunction(action.writer));
                case FLOAT -> new FloatBlockBuildingDecoder(getFloatPromotionFunction(action.writer));
                case LONG -> new LongBlockBuildingDecoder(getLongPromotionFunction(action.writer));
                case DOUBLE -> new DoubleBlockBuildingDecoder(getDoublePromotionFunction(action.writer));
                case BOOLEAN, NULL, RECORD, ENUM, ARRAY, MAP, UNION, FIXED, INT ->
                        throw new AvroTypeException("Promotion action not allowed for reader schema type " + action.reader.getType());
            };
            case WRITER_UNION -> new WriterUnionBlockBuildingDecoder((Resolver.WriterUnion) action, typeManager);
            case READER_UNION -> createBlockBuildingDecoderForAction(((Resolver.ReaderUnion) action).actualAction, typeManager);
            case ERROR -> throw new AvroTypeException("Resolution action returned with error " + action.toString());
            case SKIP -> throw new VerifyException("Skips filtered by row step");
        };
    }

    private static IOFunction<Decoder, Slice> getBytesPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case STRING -> decoder -> Slices.wrappedBuffer(decoder.readString().getBytes(StandardCharsets.UTF_8));
            default -> throw new AvroTypeException("Cannot promote type %s to bytes".formatted(writerSchema.getType()));
        };
    }

    private static IOFunction<Decoder, String> getStringPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case BYTES -> decoder -> new String(decoder.readBytes(null).array(), StandardCharsets.UTF_8);
            default -> throw new AvroTypeException("Cannot promote type %s to string".formatted(writerSchema.getType()));
        };
    }

    private static LongIoFunction<Decoder> getLongPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            default -> throw new AvroTypeException("Cannot promote type %s to long".formatted(writerSchema.getType()));
        };
    }

    private static FloatIoFunction<Decoder> getFloatPromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            default -> throw new AvroTypeException("Cannot promote type %s to float".formatted(writerSchema.getType()));
        };
    }

    private static DoubleIoFunction<Decoder> getDoublePromotionFunction(Schema writerSchema)
    {
        return switch (writerSchema.getType()) {
            case INT -> Decoder::readInt;
            case LONG -> Decoder::readLong;
            case FLOAT -> Decoder::readFloat;
            default -> throw new AvroTypeException("Cannot promote type %s to double".formatted(writerSchema.getType()));
        };
    }

    @FunctionalInterface
    private interface IOFunction<A, B>
    {
        B apply(A a)
                throws IOException;
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

    private static IoConsumer<BlockBuilder> getDefaultBlockBuilder(Schema.Field field, AvroTypeManager typeManager)
    {
        BlockBuildingDecoder buildingDecoder = createBlockBuildingDecoderForAction(Resolver.resolve(field.schema(), field.schema()), typeManager);
        byte[] defaultBytes = getDefaultByes(field);
        BinaryDecoder reuse = DecoderFactory.get().binaryDecoder(defaultBytes, null);
        return blockBuilder -> buildingDecoder.decodeIntoBlock(DecoderFactory.get().binaryDecoder(defaultBytes, reuse), blockBuilder);
    }

    private static byte[] getDefaultByes(Schema.Field field)
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
}
