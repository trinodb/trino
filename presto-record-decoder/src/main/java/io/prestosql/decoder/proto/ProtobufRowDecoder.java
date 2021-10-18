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
package io.prestosql.decoder.proto;

import com.exabeam.pipeline.models.EventOuterClass;
import com.exabeam.pipeline.models.FieldValueOuterClass;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.FieldValueProviders;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.util.function.Function.identity;

/**
 * Decoder for protobuf object.
 */
public class ProtobufRowDecoder implements RowDecoder {
    public static final String NAME = "protobuf";

    private final Set<DecoderColumnHandle> columnHandles;

    public ProtobufRowDecoder(Set<DecoderColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap) {
        try {
            EventOuterClass.Event event = EventOuterClass.Event.parseFrom(data);
            return Optional.of(columnHandles.stream()
                    .collect(toImmutableMap(identity(), e -> createDecoder(e, event))));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception " + e.getMessage());
            return Optional.empty();
        }

    }

    private FieldValueProvider createDecoder(DecoderColumnHandle t, EventOuterClass.Event event) {
        switch (t.getName()) {
            case "id":
                return new FieldValueProvider() {
                    @Override
                    public Slice getSlice() {
                        return truncateToLength(utf8Slice(event.getId()), t.getType());
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }
                };
            case "log":
                return new FieldValueProvider() {
                    @Override
                    public Slice getSlice() {
                        return truncateToLength(utf8Slice(event.getLog()), t.getType());
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }
                };
            case "approx_log_time":
                return new FieldValueProvider() {
                    @Override
                    public long getLong() {
                        return event.getApproxLogTime().getMillis() * 1000;
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }
                };
            case "parsed":
                return new FieldValueProvider() {
                    @Override
                    public boolean getBoolean() {
                        return event.getParsed();
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }
                };
            case "fields":
                return new FieldValueProvider() {
                    @Override
                    public Block getBlock() {
                        return serializeMap(event.getFieldsMap(), t.getType());
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }
                };
            default:
                return FieldValueProviders.nullValueProvider();
        }
    }

    private static Block serializeMap(Map<String, FieldValueOuterClass.FieldValue> fields, Type type) {
        List<Type> typeParameters = type.getTypeParameters();
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder entryBuilder;

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        entryBuilder = blockBuilder.beginBlockEntry();

        for (Map.Entry<String, FieldValueOuterClass.FieldValue> entry : fields.entrySet()) {
            if (entry.getKey() != null) {
                keyType.writeSlice(entryBuilder, truncateToLength(utf8Slice(entry.getKey()), keyType));
                valueType.writeSlice(entryBuilder, truncateToLength(utf8Slice(getString(entry.getValue())), keyType));
            }
        }

        blockBuilder.closeEntry();
        return (Block) type.getObject(blockBuilder, 0);
    }

    static String getString(FieldValueOuterClass.FieldValue value) {
        if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.STRINGFIELDVALUE)) {
            return value.getStringFieldValue().getValue();
        } else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.BOOLFIELDVALUE))
            return Boolean.toString((value.getBoolFieldValue().getValue()));
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.INTFIELDVALUE))
            return Integer.toString((value.getIntFieldValue().getValue()));
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.LONGFIELDVALUE))
            return Long.toString((value.getLongFieldValue().getValue()));
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.DOUBLEFIELDVALUE))
            return Double.toString((value.getDoubleFieldValue().getValue()));
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.TIMESTAMPFIELDVALUE))
            return Long.toString((value.getTimestampFieldValue().getValue().getMillis()));
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.IPV4FIELDVALUE))
            return value.getIpv4FieldValue().getValue();
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.IPV6FIELDVALUE))
            return value.getIpv6FieldValue().getValue();
        else if (value.getSealedValueCase().equals(FieldValueOuterClass.FieldValue.SealedValueCase.ARRAYFIELDVALUE))
            return value.getArrayFieldValue().getValueList().stream().map(ProtobufRowDecoder::getString).collect(Collectors.joining(", "));
        else
            return null;
    }
}
