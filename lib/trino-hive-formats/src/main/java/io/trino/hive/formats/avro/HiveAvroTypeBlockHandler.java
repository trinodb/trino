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
import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.WriterUnionBlockBuildingDecoder;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.InvalidNativeAvroLogicalType;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.NoLogicalType;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.NonNativeAvroLogicalType;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.ValidNativeAvroLogicalType;
import io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.ValidateLogicalTypeResult;
import io.trino.hive.formats.avro.model.ArrayReadAction;
import io.trino.hive.formats.avro.model.AvroLogicalType.BytesDecimalLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.DateLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.FixedDecimalLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.StringUUIDLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimeMicrosLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimeMillisLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimestampMicrosLogicalType;
import io.trino.hive.formats.avro.model.AvroLogicalType.TimestampMillisLogicalType;
import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.hive.formats.avro.model.AvroReadAction.LongIoFunction;
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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.Varchars;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.UnionToRowCoercionUtils.UNION_FIELD_TAG_TYPE;
import static io.trino.hive.formats.UnionToRowCoercionUtils.rowTypeForUnionOfTypes;
import static io.trino.hive.formats.avro.AvroHiveConstants.CHAR_TYPE_LOGICAL_NAME;
import static io.trino.hive.formats.avro.AvroHiveConstants.VARCHAR_TYPE_LOGICAL_NAME;
import static io.trino.hive.formats.avro.AvroTypeUtils.isSimpleNullableUnion;
import static io.trino.hive.formats.avro.AvroTypeUtils.unwrapNullableUnion;
import static io.trino.hive.formats.avro.AvroTypeUtils.verifyNoCircularReferences;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.MAX_ARRAY_SIZE;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.NullBlockBuildingDecoder.INSTANCE;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseBlockBuildingDecoderFor;
import static io.trino.hive.formats.avro.BaseAvroTypeBlockHandlerImpls.baseTypeFor;
import static io.trino.hive.formats.avro.HiveAvroTypeManager.getHiveLogicalVarCharOrCharType;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeBlockHandler.getLogicalTypeBuildingFunction;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.getAvroLogicalTypeSpiType;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.validateLogicalType;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class HiveAvroTypeBlockHandler
        implements AvroTypeBlockHandler
{
    private final TimestampType hiveSessionTimestamp;
    private final AtomicReference<ZoneId> convertToTimezone = new AtomicReference<>(UTC);

    public HiveAvroTypeBlockHandler(TimestampType hiveSessionTimestamp)
    {
        this.hiveSessionTimestamp = requireNonNull(hiveSessionTimestamp, "hiveSessionTimestamp is null");
    }

    @Override
    public void configure(Map<String, byte[]> fileMetadata)
    {
        if (fileMetadata.containsKey(AvroHiveConstants.WRITER_TIME_ZONE)) {
            convertToTimezone.set(ZoneId.of(new String(fileMetadata.get(AvroHiveConstants.WRITER_TIME_ZONE), StandardCharsets.UTF_8)));
        }
        else {
            // legacy path allows this conversion to be skipped with {@link org.apache.hadoop.conf.Configuration} param
            // currently no way to set that configuration in Trino
            convertToTimezone.set(TimeZone.getDefault().toZoneId());
        }
    }

    @Override
    public Type typeFor(Schema schema)
            throws AvroTypeException
    {
        verifyNoCircularReferences(schema);
        if (schema.getType() == Schema.Type.NULL) {
            // allows of dereference when no base columns from file used
            // BooleanType chosen rather arbitrarily to be stuffed with null
            // in response to behavior defined by io.trino.tests.product.hive.TestAvroSchemaStrictness.testInvalidUnionDefaults
            return BooleanType.BOOLEAN;
        }
        else if (schema.getType() == Schema.Type.UNION) {
            if (isSimpleNullableUnion(schema)) {
                return typeFor(unwrapNullableUnion(schema));
            }
            else {
                // coerce complex unions to structs
                return rowTypeForUnion(schema);
            }
        }
        ValidateLogicalTypeResult logicalTypeResult = validateLogicalType(schema);
        return switch (logicalTypeResult) {
            case NoLogicalType _ -> baseTypeFor(schema, this);
            case InvalidNativeAvroLogicalType _ -> //Hive Ignores these issues
                    baseTypeFor(schema, this);
            case NonNativeAvroLogicalType nonNativeAvroLogicalType -> switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME, CHAR_TYPE_LOGICAL_NAME -> getHiveLogicalVarCharOrCharType(schema, nonNativeAvroLogicalType);
                // logical type we don't recognize, ignore
                default -> baseTypeFor(schema, this);
            };
            case ValidNativeAvroLogicalType validNativeAvroLogicalType -> switch (validNativeAvroLogicalType.getLogicalType()) {
                case DateLogicalType dateLogicalType -> getAvroLogicalTypeSpiType(dateLogicalType);
                case BytesDecimalLogicalType bytesDecimalLogicalType -> getAvroLogicalTypeSpiType(bytesDecimalLogicalType);
                case TimestampMillisLogicalType _ -> hiveSessionTimestamp;
                // Other logical types ignored by hive/don't map to hive types
                case FixedDecimalLogicalType _, TimeMicrosLogicalType _, TimeMillisLogicalType _, TimestampMicrosLogicalType _, StringUUIDLogicalType _ -> baseTypeFor(schema, this);
            };
        };
    }

    private RowType rowTypeForUnion(Schema schema)
            throws AvroTypeException
    {
        verify(schema.isUnion());
        ImmutableList.Builder<Type> unionTypes = ImmutableList.builder();
        for (Schema variant : schema.getTypes()) {
            if (!variant.isNullable()) {
                unionTypes.add(typeFor(variant));
            }
        }
        return rowTypeForUnionOfTypes(unionTypes.build());
    }

    @Override
    public BlockBuildingDecoder blockBuildingDecoderFor(AvroReadAction readAction)
            throws AvroTypeException
    {
        ValidateLogicalTypeResult logicalTypeResult = validateLogicalType(readAction.readSchema());
        return switch (logicalTypeResult) {
            case NoLogicalType _ -> baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(readAction);
            case InvalidNativeAvroLogicalType _ -> //Hive Ignores these issues
                    baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(readAction);
            case NonNativeAvroLogicalType nonNativeAvroLogicalType -> switch (nonNativeAvroLogicalType.getLogicalTypeName()) {
                case VARCHAR_TYPE_LOGICAL_NAME ->
                        new HiveVarcharTypeBlockBuildingDecoder(getHiveLogicalVarCharOrCharType(readAction.readSchema(), nonNativeAvroLogicalType));
                case CHAR_TYPE_LOGICAL_NAME ->
                        new HiveCharTypeBlockBuildingDecoder(getHiveLogicalVarCharOrCharType(readAction.readSchema(), nonNativeAvroLogicalType));
                // logical type we don't recognize, ignore
                default -> baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(readAction);
            };
            case ValidNativeAvroLogicalType validNativeAvroLogicalType -> switch (validNativeAvroLogicalType.getLogicalType()) {
                case DateLogicalType dateLogicalType -> getLogicalTypeBuildingFunction(dateLogicalType, readAction);
                case BytesDecimalLogicalType bytesDecimalLogicalType -> getLogicalTypeBuildingFunction(bytesDecimalLogicalType, readAction);
                case TimestampMillisLogicalType _ -> new HiveCoercesedTimestampBlockBuildingDecoder(hiveSessionTimestamp, readAction, convertToTimezone);
                // Hive only supports Bytes Decimal Type
                case FixedDecimalLogicalType _ -> baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(readAction);
                // Other logical types ignored by hive/don't map to hive types
                case TimeMicrosLogicalType _, TimeMillisLogicalType _, TimestampMicrosLogicalType _, StringUUIDLogicalType _ -> baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(readAction);
            };
        };
    }

    private BlockBuildingDecoder baseBlockBuildingDecoderWithUnionCoerceAndErrorDelays(AvroReadAction readAction)
            throws AvroTypeException
    {
        return switch (readAction) {
            case NullRead _, BooleanRead _, IntRead _, LongRead _, FloatRead _, DoubleRead _, StringRead _, BytesRead _, FixedRead _, ArrayReadAction _, EnumReadAction _,
                 MapReadAction _, RecordReadAction _ -> baseBlockBuildingDecoderFor(readAction, this);
            case WrittenUnionReadAction writtenUnionReadAction -> {
                if (writtenUnionReadAction.readSchema().getType() == Schema.Type.UNION && !isSimpleNullableUnion(writtenUnionReadAction.readSchema())) {
                    yield new WriterUnionCoercedIntoRowBlockBuildingDecoder(writtenUnionReadAction, this);
                }
                else {
                    // reading a union with non-union or nullable union, optimistically try to create the reader, will fail at read time with any underlying issues
                    yield new WriterUnionBlockBuildingDecoder(writtenUnionReadAction, this);
                }
            }
            case ReadingUnionReadAction readingUnionReadAction -> {
                if (isSimpleNullableUnion(readingUnionReadAction.readSchema())) {
                    yield blockBuildingDecoderFor(readingUnionReadAction.actualAction());
                }
                else {
                    yield new ReaderUnionCoercedIntoRowBlockBuildingDecoder(readingUnionReadAction, this);
                }
            }
            case ReadErrorReadAction readErrorReadAction -> new TypeErrorThrower(readErrorReadAction);
        };
    }

    public static class WriterUnionCoercedIntoRowBlockBuildingDecoder
            extends WriterUnionBlockBuildingDecoder
    {
        private final boolean readUnionEquiv;
        private final int[] indexToChannel;
        private final int totalChannels;

        public WriterUnionCoercedIntoRowBlockBuildingDecoder(WrittenUnionReadAction writtenUnionReadAction, AvroTypeBlockHandler avroTypeManager)
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
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            int index = decoder.readIndex();
            if (readUnionEquiv) {
                // if no output channel then the schema is null and the whole record can be null;
                if (indexToChannel[index] < 0) {
                    INSTANCE.decodeIntoBlock(decoder, builder);
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

    public static class ReaderUnionCoercedIntoRowBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final BlockBuildingDecoder delegateBuilder;
        private final int outputChannel;
        private final int totalChannels;

        public ReaderUnionCoercedIntoRowBlockBuildingDecoder(ReadingUnionReadAction readingUnionReadAction, AvroTypeBlockHandler avroTypeManager)
                throws AvroTypeException
        {
            requireNonNull(readingUnionReadAction, "readerUnion is null");
            requireNonNull(avroTypeManager, "avroTypeManger is null");
            int[] indexToChannel = WriterUnionCoercedIntoRowBlockBuildingDecoder.getIndexToChannel(readingUnionReadAction.readSchema().getTypes());
            outputChannel = indexToChannel[readingUnionReadAction.firstMatch()];
            delegateBuilder = avroTypeManager.blockBuildingDecoderFor(readingUnionReadAction.actualAction());
            totalChannels = (int) IntStream.of(indexToChannel).filter(i -> i >= 0).count();
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            if (outputChannel < 0) {
                // No outputChannel for Null schema in union, null out coerces struct
                INSTANCE.decodeIntoBlock(decoder, builder);
            }
            else {
                WriterUnionCoercedIntoRowBlockBuildingDecoder
                        .makeSingleRowWithTagAndAllFieldsNullButOne(outputChannel, totalChannels, delegateBuilder, decoder, builder);
            }
        }
    }

    // A block building decoder that can delay errors till read time to allow the shape of the data to circumvent schema compatibility issues
    // for example reading a ["null", "string"] with "string" is possible if the underlying union data is all strings
    private static class TypeErrorThrower
            implements BlockBuildingDecoder
    {
        private final ReadErrorReadAction action;

        public TypeErrorThrower(ReadErrorReadAction action)
        {
            this.action = requireNonNull(action, "action is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            throw new IOException(new AvroTypeException("Resolution action returned with error " + action));
        }
    }

    private static class HiveVarcharTypeBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final Type type;

        public HiveVarcharTypeBlockBuildingDecoder(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro String with size greater than %s. Found String size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            type.writeSlice(builder, Varchars.truncateToLength(Slices.wrappedBuffer(bytes), type));
        }
    }

    private static class HiveCharTypeBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final Type type;

        public HiveCharTypeBlockBuildingDecoder(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            long size = decoder.readLong();
            if (size > MAX_ARRAY_SIZE) {
                throw new IOException("Unable to read avro String with size greater than %s. Found String size: %s".formatted(MAX_ARRAY_SIZE, size));
            }
            byte[] bytes = new byte[(int) size];
            decoder.readFixed(bytes);
            type.writeSlice(builder, Chars.truncateToLengthAndTrimSpaces(Slices.wrappedBuffer(bytes), type));
        }
    }

    private static class HiveCoercesedTimestampBlockBuildingDecoder
            implements BlockBuildingDecoder
    {
        private final TimestampType hiveSessionTimestamp;
        private final LongIoFunction<Decoder> longDecoder;
        private final AtomicReference<ZoneId> convertToTimezone;

        public HiveCoercesedTimestampBlockBuildingDecoder(TimestampType hiveSessionTimestamp, AvroReadAction avroReadAction, AtomicReference<ZoneId> convertToTimezone)
        {
            checkArgument(avroReadAction instanceof LongRead, "TimestampMillis type can only be read from Long Avro type");
            this.hiveSessionTimestamp = requireNonNull(hiveSessionTimestamp, "hiveSessionTimestamp is null");
            longDecoder = ((LongRead) avroReadAction).getLongDecoder();
            this.convertToTimezone = requireNonNull(convertToTimezone, "convertToTimezone is null");
        }

        @Override
        public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
                throws IOException
        {
            long millisSinceEpochUTC = longDecoder.apply(decoder);
            if (hiveSessionTimestamp.isShort()) {
                hiveSessionTimestamp.writeLong(builder, DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND);
            }
            else {
                LongTimestamp longTimestamp = new LongTimestamp(DateTimeZone.forTimeZone(TimeZone.getTimeZone(convertToTimezone.get())).convertUTCToLocal(millisSinceEpochUTC) * Timestamps.MICROSECONDS_PER_MILLISECOND, 0);
                hiveSessionTimestamp.writeObject(builder, longTimestamp);
            }
        }
    }
}
