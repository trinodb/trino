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
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.DUMMY_FIELD_NAME;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.DUMMY_ROW_TYPE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class EmptyFieldHandlingAvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";

    private static final FieldValueProvider NULL_FIELD_VALUE_PROVIDER = new FieldValueProvider()
    {
        private final Block nullBlock = getNullBlock();

        private Block getNullBlock()
        {
            RowBlockBuilder blockBuilder = DUMMY_ROW_TYPE.createBlockBuilder(null, 1);
            blockBuilder.buildEntry(fieldBuilders -> {
                fieldBuilders.get(0).appendNull();
            });
            return blockBuilder.build().getObject(0, Block.class);
        }

        @Override
        public Block getBlock()
        {
            return nullBlock;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    };
    private final RowDecoder delegate;

    public EmptyFieldHandlingAvroRowDecoder(RowDecoder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data)
    {
        return delegate.decodeRow(data).map(decodedRow -> decodedRow.entrySet().stream()
                .map(entry -> {
                    if (isDummyRowType(entry.getKey().getType())) {
                        return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), NULL_FIELD_VALUE_PROVIDER);
                    }
                    else {
                        return entry;
                    }
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    // An empty field encoded as a dummy row has exactly one field named "$dummy" with boolean type.
    private boolean isDummyRowType(Type type)
    {
        if (!(type instanceof RowType)) {
            return false;
        }
        RowType rowType = (RowType) type;
        if (rowType.getFields().size() != 1) {
            return false;
        }
        RowType.Field field = rowType.getFields().get(0);
        return field.getType() == BOOLEAN && field.getName().map(name -> name.equals(DUMMY_FIELD_NAME)).orElse(false);
    }
}
