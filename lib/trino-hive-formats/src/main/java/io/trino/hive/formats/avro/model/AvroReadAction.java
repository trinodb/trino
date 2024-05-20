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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.AvroPageDataReader.RowBlockBuildingDecoder.SkipSchemaBuildingAction.createSkipActionForSchema;
import static io.trino.hive.formats.avro.AvroPageDataReader.getDefaultByes;
import static java.util.Objects.requireNonNull;

public sealed interface AvroReadAction
        permits
        NullRead,
        BooleanRead,
        IntRead,
        LongRead,
        FloatRead,
        DoubleRead,
        StringRead,
        BytesRead,
        FixedRead,
        ArrayReadAction,
        EnumReadAction,
        MapReadAction,
        ReadingUnionReadAction,
        RecordReadAction,
        WrittenUnionReadAction,
        ReadErrorReadAction
{
    Schema readSchema();

    Schema writeSchema();

    static AvroReadAction fromAction(Resolver.Action action)
            throws AvroTypeException
    {
        return switch (action.type) {
            case DO_NOTHING -> switch (action.reader.getType()) {
                case NULL -> new NullRead(action.reader, action.writer);
                case BOOLEAN -> new BooleanRead(action.reader, action.writer);
                case INT -> new IntRead(action.reader, action.writer);
                case LONG -> new LongRead(action.reader, action.writer);
                case FLOAT -> new FloatRead(action.reader, action.writer);
                case DOUBLE -> new DoubleRead(action.reader, action.writer);
                case STRING -> new StringRead(action.reader, action.writer);
                case BYTES -> new BytesRead(action.reader, action.writer);
                case FIXED -> new FixedRead(action.reader, action.writer);
                // these reader types covered by special action types
                case ENUM, ARRAY, MAP, RECORD, UNION -> throw new IllegalStateException("Underlying Avro Library change detected with action: " + action);
            };
            case PROMOTE -> switch (action.reader.getType()) {
                // only certain types valid to promote into as determined by org.apache.avro.Resolver.Promote.isValid
                case LONG -> new LongRead(action.reader, action.writer);
                case FLOAT -> new FloatRead(action.reader, action.writer);
                case DOUBLE -> new DoubleRead(action.reader, action.writer);
                case STRING -> {
                    if (action.writer.getType() == Schema.Type.BYTES) {
                        yield new StringRead(action.reader, action.writer);
                    }
                    throw new IllegalStateException("Unable to promote to String from type " + action.writer.getType());
                }
                case BYTES -> {
                    if (action.writer.getType() == Schema.Type.STRING) {
                        yield new BytesRead(action.reader, action.writer);
                    }
                    throw new IllegalStateException("Unable to promote to Bytes from type " + action.writer.getType());
                }
                case NULL, BOOLEAN, INT, FIXED, ENUM, ARRAY, MAP, RECORD, UNION ->
                        throw new IllegalStateException("Promotion action not allowed for reader schema type " + action.reader.getType());
            };
            case CONTAINER -> switch (action.reader.getType()) {
                case ARRAY -> new ArrayReadAction(action.reader, action.writer, fromAction(((Resolver.Container) action).elementAction));
                case MAP -> new MapReadAction(action.reader, action.writer, fromAction(((Resolver.Container) action).elementAction));
                default -> throw new IllegalStateException("Not possible to have container action type with non container reader schema " + action.reader.getType());
            };
            case ENUM -> new EnumReadAction(action.reader, action.writer, getSymbolIndex((Resolver.EnumAdjust) action));
            case RECORD -> recordReadAction(action.reader, action.writer, ((Resolver.RecordAdjust) action));
            case WRITER_UNION -> fromWrittenUnionAction((Resolver.WriterUnion) action);
            case READER_UNION -> fromReaderUnionAction((Resolver.ReaderUnion) action);
            case ERROR -> new ReadErrorReadAction(action.reader, action.writer, ((Resolver.ErrorAction) action).error, action.toString());
            case SKIP -> throw new IllegalStateException("Skips recordReadAction");
        };
    }

    private static RecordReadAction recordReadAction(Schema readSchema, Schema writeSchema, Resolver.RecordAdjust recordAdjust)
            throws AvroTypeException
    {
        RecordFieldReadAction[] buildSteps = new RecordFieldReadAction[recordAdjust.fieldActions.length + recordAdjust.readerOrder.length
                - recordAdjust.firstDefault];
        int i = 0;
        int readerFieldCount = 0;
        for (; i < recordAdjust.fieldActions.length; i++) {
            Resolver.Action fieldAction = recordAdjust.fieldActions[i];
            if (fieldAction instanceof Resolver.Skip skip) {
                buildSteps[i] = new SkipFieldRecordFieldReadAction(createSkipActionForSchema(skip.writer));
            }
            else {
                Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
                buildSteps[i] = new ReadFieldAction(fromAction(fieldAction), readField.pos());
            }
        }

        // add defaulting if required
        for (; i < buildSteps.length; i++) {
            // create constant block
            Schema.Field readField = recordAdjust.readerOrder[readerFieldCount++];
            buildSteps[i] = new DefaultValueFieldRecordFieldReadAction(readField.schema(), getDefaultByes(readField), readField.pos());
        }

        verify(Arrays.stream(buildSteps)
                        .mapToInt(fieldAction -> switch (fieldAction) {
                            case DefaultValueFieldRecordFieldReadAction a -> a.outputChannel();
                            case ReadFieldAction b -> b.outputChannel();
                            case SkipFieldRecordFieldReadAction ignore -> -1;
                        }).filter(a -> a >= 0)
                        .distinct()
                        .sum() == (recordAdjust.reader.getFields().size() * (recordAdjust.reader.getFields().size() - 1) / 2),
                "Every channel in output block builder must be accounted for");
        verify(Arrays.stream(buildSteps)
                .mapToInt(fieldAction -> switch (fieldAction) {
                    case DefaultValueFieldRecordFieldReadAction a -> a.outputChannel();
                    case ReadFieldAction b -> b.outputChannel();
                    case SkipFieldRecordFieldReadAction ignore -> -1;
                })
                .filter(a -> a >= 0)
                .distinct().count() == (long) recordAdjust.reader.getFields().size(), "Every channel in output block builder must be accounted for");
        return new RecordReadAction(readSchema, writeSchema, ImmutableList.copyOf(buildSteps));
    }

    private static List<Slice> getSymbolIndex(Resolver.EnumAdjust action)
            throws AvroTypeException
    {
        List<String> symbolsList = requireNonNull(action, "action is null").reader.getEnumSymbols();
        Slice[] symbols = symbolsList.stream().map(Slices::utf8Slice).toArray(Slice[]::new);
        if (!action.noAdjustmentsNeeded) {
            Slice[] adjustedSymbols = new Slice[action.writer.getEnumSymbols().size()];
            for (int i = 0; i < action.adjustments.length; i++) {
                if (action.adjustments[i] < 0) {
                    throw new AvroTypeException("No reader Enum value for writer Enum value " + action.writer.getEnumSymbols().get(i));
                }
                adjustedSymbols[i] = symbols[action.adjustments[i]];
            }
            symbols = adjustedSymbols;
        }
        return ImmutableList.copyOf(symbols);
    }

    private static WrittenUnionReadAction fromWrittenUnionAction(Resolver.WriterUnion writerUnion)
            throws AvroTypeException
    {
        AvroReadAction[] readActions = new AvroReadAction[writerUnion.actions.length];
        for (int i = 0; i < writerUnion.actions.length; i++) {
            readActions[i] = fromAction(writerUnion.actions[i]);
        }
        return new WrittenUnionReadAction(writerUnion.reader, writerUnion.writer, writerUnion.unionEquiv, ImmutableList.copyOf(readActions));
    }

    private static ReadingUnionReadAction fromReaderUnionAction(Resolver.ReaderUnion readerUnion)
            throws AvroTypeException
    {
        return new ReadingUnionReadAction(readerUnion.reader, readerUnion.writer, readerUnion.firstMatch, fromAction(readerUnion.actualAction));
    }
}
