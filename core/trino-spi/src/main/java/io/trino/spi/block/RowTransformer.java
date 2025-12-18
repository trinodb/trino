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
package io.trino.spi.block;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public class RowTransformer
{
    private final RowType rowType;
    private final ValueBlock[] blocks;

    private RowTransformer(final RowType rowType, final SqlRow row)
    {
        this.rowType = rowType;
        this.blocks = sqlRowToValueBlocks(row);
    }

    public static BiFunction<Type, Block, Block> setNativeValue(final Object value)
    {
        return (type, _) -> writeNativeValue(type, value);
    }

    public static BiFunction<Type, ArrayBlock, ArrayBlock> appendNativeValue(final Object value)
    {
        return (type, original) -> {
            final Block elementsBlock = original.getElementsBlock();
            final ArrayBlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(type, null, elementsBlock.getPositionCount());
            arrayBlockBuilder.buildEntry(arrayValueBuilder -> {
                for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
                    arrayValueBuilder.append(elementsBlock.getSingleValueBlock(i), 0);
                }

                final ValueBlock addedValue = writeNativeValue(type, value);
                arrayValueBuilder.append(addedValue, 0);
            });
            return (ArrayBlock) arrayBlockBuilder.build();
        };
    }

    private static <B extends Block> void transform(final Block[] blocks, final RowType rowType, final BiFunction<Type, B, B> transform, final List<String> path)
    {
        final List<String> mutablePath = new ArrayList<>(path);
        final IndexedField indexedField = findField(rowType, mutablePath.removeFirst());

        if (mutablePath.isEmpty()) {
            Type fieldType = indexedField.field().getType();
            if (fieldType instanceof final ArrayType arrayType) {
                fieldType = arrayType.getElementType();
            }
            blocks[indexedField.index()] = transform.apply(fieldType, (B) blocks[indexedField.index()]);
        }
        else if (blocks[indexedField.index()] instanceof final RowBlock rowBlock && indexedField.field().getType() instanceof final RowType innerRowType) {
            final SqlRow innerSqlRow = rowBlock.getRow(0);
            final Block[] innerBlocks = sqlRowToValueBlocks(innerSqlRow);
            transform(innerBlocks, innerRowType, transform, mutablePath);
            blocks[indexedField.index()] = RowBlock.fromFieldBlocks(rowBlock.getPositionCount(), innerBlocks);
        }
        else if (indexedField.field().getType() instanceof final ArrayType arrayType && arrayType.getElementType() instanceof final RowType arrayRowType && blocks[indexedField.index()] instanceof final ArrayBlock arrayBlock) {
            final RowBlock[] elements = arrayBlockToRowBlocks(arrayBlock);
            final ArrayBlockBuilder arrayBlockBuilder = new ArrayBlockBuilder(arrayRowType, null, elements.length);
            arrayBlockBuilder.buildEntry(arrayValueBuilder -> {
                for (RowBlock element : elements) {
                    final Block[] rowValues = sqlRowToValueBlocks(element.getRow(0));
                    transform(rowValues, arrayRowType, transform, mutablePath);

                    final RowBlock newRowBlock = RowBlock.fromFieldBlocks(element.getPositionCount(), rowValues);
                    arrayValueBuilder.append(newRowBlock, 0);
                }
            });

            blocks[indexedField.index()] = arrayBlockBuilder.build();
        }
    }

    private static RowBlock[] arrayBlockToRowBlocks(final ArrayBlock array)
    {
        final Block elementsBlock = array.getElementsBlock(); // Can be an RowBlock
        final RowBlock[] elements = new RowBlock[elementsBlock.getPositionCount()];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = (RowBlock) elementsBlock.getSingleValueBlock(i);
        }
        return elements;
    }

    private static ValueBlock[] sqlRowToValueBlocks(final SqlRow sqlRow)
    {
        final ValueBlock[] blocks = new ValueBlock[sqlRow.getFieldCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = sqlRow.getRawFieldBlock(i).getSingleValueBlock(sqlRow.getRawIndex());
        }
        return blocks;
    }

    private static IndexedField findField(final RowType rowType, final String name)
    {
        final List<RowType.Field> fields = rowType.getFields();
        final Block[] blocks = new Block[fields.size()];
        for (int i = 0; i < blocks.length; i++) {
            final RowType.Field field = fields.get(i);
            if (field.getName().orElse("").equals(name)) {
                return new IndexedField(i, field);
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, String.format("Unknown field %s", name));
    }

    public <B extends Block> void transform(final BiFunction<Type, B, B> transform, final String... path)
    {
        transform(blocks, rowType, transform, Arrays.asList(path));
    }

    private SqlRow build()
    {
        return new SqlRow(0, blocks);
    }

    public static SqlRow build(final RowType rowType, final SqlRow row, final Consumer<RowTransformer> entryBuilder)
    {
        final RowTransformer transformer = new RowTransformer(rowType, row);
        entryBuilder.accept(transformer);
        return transformer.build();
    }

    public static Block build(final RowType rowType, final RowBlock inputArray, final Consumer<RowTransformer> entryBuilder)
    {
        final RowBlockBuilder rowBlockBuilder = new RowBlockBuilder(rowType.getTypeParameters(), null, inputArray.getPositionCount());
        for (int index = 0; index < inputArray.getPositionCount(); index++) {
            final SqlRow newRow = build(rowType, inputArray.getRow(index), entryBuilder);

            rowBlockBuilder.buildEntry(fieldBuilders -> {
                for (int fieldIndex = 0; fieldIndex < fieldBuilders.size(); fieldIndex++) {
                    fieldBuilders.get(fieldIndex).append(newRow.getUnderlyingFieldBlock(fieldIndex), 0);
                }
            });
        }
        return rowBlockBuilder.build();
    }

    private record IndexedField(int index, RowType.Field field)
    {
    }
}
