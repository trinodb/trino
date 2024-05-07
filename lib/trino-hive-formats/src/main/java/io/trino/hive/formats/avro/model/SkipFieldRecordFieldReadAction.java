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
package io.trino.hive.formats.avro.model;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public record SkipFieldRecordFieldReadAction(SkipAction skipAction)
        implements RecordFieldReadAction
{
    public SkipFieldRecordFieldReadAction
    {
        requireNonNull(skipAction, "skipAction is null");
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
            implements SkipFieldRecordFieldReadAction.SkipAction
    {
        private final SkipFieldRecordFieldReadAction.SkipAction valueSkipAction;

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
            implements SkipFieldRecordFieldReadAction.SkipAction
    {
        private final SkipFieldRecordFieldReadAction.SkipAction[] fieldSkips;

        public RecordSkipAction(List<Schema.Field> fields)
        {
            fieldSkips = new SkipFieldRecordFieldReadAction.SkipAction[requireNonNull(fields, "fields is null").size()];
            for (int i = 0; i < fields.size(); i++) {
                fieldSkips[i] = createSkipActionForSchema(fields.get(i).schema());
            }
        }

        @Override
        public void skip(Decoder decoder)
                throws IOException
        {
            for (SkipFieldRecordFieldReadAction.SkipAction fieldSkipAction : fieldSkips) {
                fieldSkipAction.skip(decoder);
            }
        }
    }

    private static class UnionSkipAction
            implements SkipFieldRecordFieldReadAction.SkipAction
    {
        private final SkipFieldRecordFieldReadAction.SkipAction[] skipActions;

        private UnionSkipAction(List<Schema> types)
        {
            skipActions = new SkipFieldRecordFieldReadAction.SkipAction[requireNonNull(types, "types is null").size()];
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
