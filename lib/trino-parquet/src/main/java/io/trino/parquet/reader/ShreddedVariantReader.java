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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.parquet.reader.ShreddedValue.ShreddedArray;
import io.trino.parquet.reader.ShreddedValue.ShreddedObject;
import io.trino.parquet.reader.ShreddedValue.ShreddedScalar;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ShreddedVariantField.METADATA;
import static io.trino.parquet.ShreddedVariantField.TYPED_VALUE;
import static io.trino.parquet.ShreddedVariantField.VALUE;
import static io.trino.parquet.reader.VariantShreddingReconstructor.reconstruct;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;

/**
 * Reads a shredded Variant column that has been materialized (by the standard nested Parquet
 * readers) as a struct block mirroring the Parquet shredding layout, and reconstructs the
 * unshredded Variant for each row via {@link VariantShreddingReconstructor}.
 * <p>
 * The struct type of a Variant node is a {@link RowType} with a {@code value} field (optional
 * {@code VARBINARY}) and a {@code typed_value} field; the top-level node additionally has a
 * {@code metadata} field. A {@code typed_value} of {@link RowType} is a shredded object (one field
 * per shredded key, each itself a node), of {@link io.trino.spi.type.ArrayType} is a shredded array
 * (element is a node), and otherwise is a shredded scalar.
 */
public final class ShreddedVariantReader
{
    private ShreddedVariantReader() {}

    /**
     * Reconstructs the Variant for every position of a shredded Variant column, where empty entries
     * are SQL nulls.
     */
    public static List<Optional<Variant>> reconstructColumn(RowType structType, Block structBlock, int positionCount)
    {
        int metadataIndex = fieldIndex(structType, METADATA);
        checkArgument(metadataIndex >= 0, "Shredded Variant column is missing the metadata field");
        List<Block> fields = getRowFieldsFromBlock(structBlock);
        Block metadataBlock = fields.get(metadataIndex);

        ImmutableList.Builder<Optional<Variant>> variants = ImmutableList.builderWithExpectedSize(positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (structBlock.isNull(position)) {
                variants.add(Optional.empty());
                continue;
            }
            Metadata metadata = Metadata.from(VARBINARY.getSlice(metadataBlock, position));
            ShreddedValue node = walkNode(structType, fields, position);
            variants.add(reconstruct(node, metadata));
        }
        return variants.build();
    }

    /**
     * Builds the {@link ShreddedValue} for a single node at {@code position}. Any {@code metadata}
     * field is ignored; only {@code value} and {@code typed_value} are read.
     */
    static ShreddedValue walkNode(RowType nodeType, List<Block> nodeFields, int position)
    {
        Optional<Slice> value = sliceField(nodeType, nodeFields, VALUE, position);

        int typedValueIndex = fieldIndex(nodeType, TYPED_VALUE);
        if (typedValueIndex < 0) {
            return new ShreddedScalar(value, Optional.empty());
        }
        Type typedValueType = nodeType.getFields().get(typedValueIndex).getType();
        Block typedValueBlock = nodeFields.get(typedValueIndex);
        boolean typedValueNull = typedValueBlock.isNull(position);

        if (typedValueType instanceof RowType objectType) {
            if (typedValueNull) {
                return new ShreddedObject(value, Optional.empty());
            }
            List<Block> objectFields = getRowFieldsFromBlock(typedValueBlock);
            Map<Slice, ShreddedValue> shreddedFields = new LinkedHashMap<>();
            List<RowType.Field> fields = objectType.getFields();
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                Slice name = utf8Slice(field.getName().orElseThrow());
                if (!(field.getType() instanceof RowType childType)) {
                    throw new IllegalArgumentException("Shredded object field '%s' is not a Variant node: %s".formatted(name.toStringUtf8(), field.getType()));
                }
                List<Block> childFields = getRowFieldsFromBlock(objectFields.get(i));
                shreddedFields.put(name, walkNode(childType, childFields, position));
            }
            return new ShreddedObject(value, Optional.of(shreddedFields));
        }

        if (typedValueType instanceof ArrayType arrayType) {
            if (typedValueNull) {
                return new ShreddedArray(value, Optional.empty());
            }
            ColumnarArray array = toColumnarArray(typedValueBlock);
            int offset = array.getOffset(position);
            int length = array.getLength(position);
            if (!(arrayType.getElementType() instanceof RowType elementType)) {
                throw new IllegalArgumentException("Shredded array element is not a Variant node: " + arrayType.getElementType());
            }
            List<Block> elementFields = getRowFieldsFromBlock(array.getElementsBlock());
            ImmutableList.Builder<ShreddedValue> elements = ImmutableList.builderWithExpectedSize(length);
            for (int i = 0; i < length; i++) {
                elements.add(walkNode(elementType, elementFields, offset + i));
            }
            return new ShreddedArray(value, Optional.of(elements.build()));
        }

        if (typedValueNull) {
            return new ShreddedScalar(value, Optional.empty());
        }
        return new ShreddedScalar(value, Optional.of(scalarToVariant(typedValueType, typedValueBlock, position)));
    }

    private static Variant scalarToVariant(Type type, Block block, int position)
    {
        return switch (type) {
            case BigintType _ -> Variant.ofLong(type.getLong(block, position));
            case IntegerType _ -> Variant.ofInt(toIntExact(type.getLong(block, position)));
            case SmallintType _ -> Variant.ofShort((short) type.getLong(block, position));
            case TinyintType _ -> Variant.ofByte((byte) type.getLong(block, position));
            case RealType realType -> Variant.ofFloat(realType.getFloat(block, position));
            case DoubleType _ -> Variant.ofDouble(type.getDouble(block, position));
            case BooleanType _ -> Variant.ofBoolean(type.getBoolean(block, position));
            case DateType _ -> Variant.ofDate(toIntExact(type.getLong(block, position)));
            case DecimalType decimalType -> Variant.ofDecimal(Decimals.readBigDecimal(decimalType, block, position));
            case VarcharType _ -> Variant.ofString(type.getSlice(block, position));
            case VarbinaryType _ -> Variant.ofBinary(type.getSlice(block, position));
            default -> throw new UnsupportedOperationException("Unsupported shredded Variant scalar type: " + type);
        };
    }

    private static Optional<Slice> sliceField(RowType nodeType, List<Block> nodeFields, String name, int position)
    {
        int index = fieldIndex(nodeType, name);
        if (index < 0) {
            return Optional.empty();
        }
        Block block = nodeFields.get(index);
        if (block.isNull(position)) {
            return Optional.empty();
        }
        return Optional.of(VARBINARY.getSlice(block, position));
    }

    private static int fieldIndex(RowType rowType, String name)
    {
        List<RowType.Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().map(name::equals).orElse(false)) {
                return i;
            }
        }
        return -1;
    }
}
