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
package io.trino.plugin.kafka.decoder;

import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EMPTY_FIELD_MARKER;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EMPTY_FIELD_MARKER_TYPE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class EmptyFieldHandlingAvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";

    private static final SqlRow EMPTY_FIELD_MARKER_ROW;

    static {
        RowBlockBuilder blockBuilder = EMPTY_FIELD_MARKER_TYPE.createBlockBuilder(null, 1);
        blockBuilder.buildEntry(fieldBuilders -> {
            fieldBuilders.get(0).appendNull();
        });
        EMPTY_FIELD_MARKER_ROW = blockBuilder.build().getObject(0, SqlRow.class);
    }

    private static final FieldValueProvider EMPY_FIELD_MARKER_VALUE_PROVIDER = new EmptyFieldMarkerValueProvider();

    private final RowDecoder delegate;

    public EmptyFieldHandlingAvroRowDecoder(RowDecoder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data)
    {
        return delegate.decodeRow(data).map(decodedRow -> decodedRow.entrySet().stream()
                .map(this::replaceEmptyFieldMarkerValueProvider)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Map.Entry<DecoderColumnHandle, FieldValueProvider> replaceEmptyFieldMarkerValueProvider(Map.Entry<DecoderColumnHandle, FieldValueProvider> field)
    {
        if (isEmptyFieldMarkerType(field.getKey().getType())) {
            return new AbstractMap.SimpleImmutableEntry<>(field.getKey(), EMPY_FIELD_MARKER_VALUE_PROVIDER);
        }
        return field;
    }

    // An avro record with no fields is encoded as a row which has exactly one field named "$empty_field_marker" with boolean type.
    private boolean isEmptyFieldMarkerType(Type type)
    {
        if (!(type instanceof RowType)) {
            return false;
        }
        RowType rowType = (RowType) type;
        if (rowType.getFields().size() != 1) {
            return false;
        }
        RowType.Field field = rowType.getFields().get(0);
        return field.getType() == BOOLEAN && field.getName().map(name -> name.equals(EMPTY_FIELD_MARKER)).orElse(false);
    }

    private static final class EmptyFieldMarkerValueProvider
            extends FieldValueProvider
    {
        @Override
        public Object getObject()
        {
            return EMPTY_FIELD_MARKER_ROW;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    }
}
