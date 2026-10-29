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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static org.apache.iceberg.types.Type.TypeID.UNKNOWN;

final class UnknownTypes
{
    private UnknownTypes() {}

    static boolean isWriteSupported(Schema schema)
    {
        return isWriteSupported(schema.asStruct());
    }

    private static boolean isWriteSupported(org.apache.iceberg.types.Type type)
    {
        // Unknown fields are supported at the top level and inside structs, but not inside lists and maps.
        // At least one field must remain in each struct after removing unknown fields.
        return switch (type.typeId()) {
            case STRUCT -> {
                List<Types.NestedField> fields = type.asStructType().fields();
                yield fields.stream().anyMatch(field -> field.type().typeId() != UNKNOWN) &&
                        fields.stream().allMatch(field -> isWriteSupported(field.type()));
            }
            case LIST -> !containsUnknown(type.asListType().elementType());
            case MAP -> !containsUnknown(type.asMapType().keyType()) && !containsUnknown(type.asMapType().valueType());
            default -> true;
        };
    }

    static boolean containsUnknown(org.apache.iceberg.types.Type type)
    {
        return switch (type.typeId()) {
            case UNKNOWN -> true;
            case STRUCT -> type.asStructType().fields().stream().anyMatch(field -> containsUnknown(field.type()));
            case LIST -> containsUnknown(type.asListType().elementType());
            case MAP -> containsUnknown(type.asMapType().keyType()) || containsUnknown(type.asMapType().valueType());
            default -> false;
        };
    }

    static Schema removeUnknown(Schema schema)
    {
        if (!containsUnknown(schema.asStruct())) {
            return schema;
        }
        return new Schema(removeUnknown(schema.asStruct()).fields());
    }

    private static Types.StructType removeUnknown(Types.StructType struct)
    {
        return Types.StructType.of(struct.fields().stream()
                .filter(field -> field.type().typeId() != UNKNOWN)
                .map(field -> {
                    org.apache.iceberg.types.Type type = field.type();
                    if (!(type instanceof Types.StructType nested)) {
                        return field;
                    }
                    return Types.NestedField.from(field)
                            .ofType(removeUnknown(nested))
                            .build();
                })
                .collect(toImmutableList()));
    }

    static Page removeUnknown(Page page, List<Type> types, Schema schema)
    {
        List<Types.NestedField> fields = schema.columns();
        Block[] blocks = new Block[page.getChannelCount()];
        int count = 0;
        for (int channel = 0; channel < fields.size(); channel++) {
            org.apache.iceberg.types.Type icebergType = fields.get(channel).type();
            if (icebergType.typeId() != UNKNOWN) {
                blocks[count++] = removeUnknown(page.getBlock(channel), types.get(channel), icebergType);
            }
        }
        Block[] result = new Block[count];
        System.arraycopy(blocks, 0, result, 0, count);
        return new Page(page.getPositionCount(), result);
    }

    private static Block removeUnknown(Block block, Type type, org.apache.iceberg.types.Type icebergType)
    {
        if (!(icebergType instanceof Types.StructType struct) || !containsUnknown(struct)) {
            return block;
        }
        RowType rowType = (RowType) type;
        List<Block> fieldBlocks = getRowFieldsFromBlock(block);
        ImmutableList.Builder<Block> newFieldBlocks = ImmutableList.builder();
        for (int i = 0; i < struct.fields().size(); i++) {
            Types.NestedField field = struct.fields().get(i);
            if (field.type().typeId() != UNKNOWN) {
                newFieldBlocks.add(removeUnknown(fieldBlocks.get(i), rowType.getFields().get(i).getType(), field.type()));
            }
        }

        int positionCount = block.getPositionCount();
        Optional<long[]> valueIsValid = Optional.empty();
        if (block.mayHaveNull()) {
            long[] words = Bitmap.allocateWords(positionCount, false);
            for (int position = 0; position < positionCount; position++) {
                if (!block.isNull(position)) {
                    Bitmap.set(words, 0, position);
                }
            }
            valueIsValid = Optional.of(words);
        }
        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, valueIsValid, newFieldBlocks.build().toArray(new Block[0]));
    }
}
