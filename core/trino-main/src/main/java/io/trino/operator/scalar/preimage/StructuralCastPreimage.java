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
package io.trino.operator.scalar.preimage;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_NOT_DISTINCT_FROM;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/// Preserves native composite comparison semantics by mapping every constant element exactly.
/// Nested nulls remain in native values; they are never approximated by two-valued domains.
public final class StructuralCastPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        return Optional.empty();
    }

    @Override
    public Optional<NullableValue> comparisonConstant(Context context, NullableValue constant)
    {
        if (context.signature().getArgumentTypes().size() != 1) {
            return Optional.empty();
        }
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = constant.getType();
        if (!(source instanceof ArrayType && target instanceof ArrayType) && !(source instanceof RowType && target instanceof RowType) && !(source instanceof MapType && target instanceof MapType)) {
            return Optional.empty();
        }
        return map(context, source, target, constant.getValue());
    }

    private static Optional<NullableValue> map(Context context, Type source, Type target, Object value)
    {
        if (!source.isComparable() || !target.isComparable() || !context.functions().canCoerce(source, target)) {
            return Optional.empty();
        }
        if (value == null || source.equals(target)) {
            return Optional.of(new NullableValue(source, value));
        }
        if (source instanceof ArrayType sourceArray && target instanceof ArrayType targetArray) {
            Block array = (Block) value;
            BlockBuilder result = sourceArray.getElementType().createBlockBuilder(null, array.getPositionCount());
            for (int position = 0; position < array.getPositionCount(); position++) {
                Optional<NullableValue> element = map(context, sourceArray.getElementType(), targetArray.getElementType(), readNativeValue(targetArray.getElementType(), array, position));
                if (element.isEmpty()) {
                    return Optional.empty();
                }
                writeNativeValue(sourceArray.getElementType(), result, element.get().getValue());
            }
            return Optional.of(new NullableValue(source, result.build()));
        }
        if (source instanceof RowType sourceRow && target instanceof RowType targetRow) {
            SqlRow row = (SqlRow) value;
            Block[] fields = new Block[sourceRow.getFieldTypes().size()];
            for (int index = 0; index < fields.length; index++) {
                Type sourceField = sourceRow.getFieldTypes().get(index);
                Type targetField = targetRow.getFieldTypes().get(index);
                Optional<NullableValue> field = map(context, sourceField, targetField, readNativeValue(targetField, row.getRawFieldBlock(index), row.getRawIndex()));
                if (field.isEmpty()) {
                    return Optional.empty();
                }
                BlockBuilder result = sourceField.createBlockBuilder(null, 1);
                writeNativeValue(sourceField, result, field.get().getValue());
                fields[index] = result.build();
            }
            return Optional.of(new NullableValue(source, new SqlRow(0, fields)));
        }
        if (source instanceof MapType sourceMap && target instanceof MapType targetMap) {
            SqlMap map = (SqlMap) value;
            BlockBuilder keys = sourceMap.getKeyType().createBlockBuilder(null, map.getSize());
            BlockBuilder values = sourceMap.getValueType().createBlockBuilder(null, map.getSize());
            for (int index = 0; index < map.getSize(); index++) {
                int position = map.getRawOffset() + index;
                Optional<NullableValue> key = map(context, sourceMap.getKeyType(), targetMap.getKeyType(), readNativeValue(targetMap.getKeyType(), map.getRawKeyBlock(), position));
                Optional<NullableValue> entry = map(context, sourceMap.getValueType(), targetMap.getValueType(), readNativeValue(targetMap.getValueType(), map.getRawValueBlock(), position));
                if (key.isEmpty() || entry.isEmpty()) {
                    return Optional.empty();
                }
                writeNativeValue(sourceMap.getKeyType(), keys, key.get().getValue());
                writeNativeValue(sourceMap.getValueType(), values, entry.get().getValue());
            }
            // Exact mappings preserve key identity, so distinct target keys stay distinct.
            return Optional.of(new NullableValue(source, new SqlMap(sourceMap, STRICT_NOT_DISTINCT_FROM, keys.build(), values.build())));
        }
        // Cross-family character casts need operator-specific treatment, so they cannot
        // promise to preserve every comparison with one mapped constant.
        if (!source.isOrderable() || !target.isOrderable() ||
                (source instanceof CharType && target instanceof VarcharType) || (source instanceof VarcharType && target instanceof CharType) ||
                !CastPreimages.eligible(context, source, target) || !CastPreimages.injectiveAt(context, source, target, value)) {
            return Optional.empty();
        }
        if (target instanceof TimestampWithTimeZoneType timestamp) {
            value = timestamp.isShort()
                    ? packDateTimeWithZone(unpackMillisUtc((long) value), context.session().getTimeZoneKey())
                    : LongTimestampWithTimeZone.fromEpochMillisAndFraction(((LongTimestampWithTimeZone) value).getEpochMillis(), ((LongTimestampWithTimeZone) value).getPicosOfMilli(), context.session().getTimeZoneKey());
        }
        Optional<Function<Object, Object>> reverse = context.functions().coercion(target, source);
        Optional<Function<Object, Object>> forward = context.functions().coercion(source, target);
        if (reverse.isEmpty() || forward.isEmpty()) {
            return Optional.empty();
        }
        try {
            Object input = reverse.get().apply(value);
            Object output = forward.get().apply(input);
            // Range deliberately rejects scalar NaN, but widening REAL to DOUBLE preserves it.
            boolean equal = source.equals(REAL) && target.equals(DOUBLE) && Double.isNaN((double) value)
                    ? Double.isNaN((double) output)
                    : Range.equal(target, value).overlaps(Range.equal(target, output));
            return equal ? Optional.of(new NullableValue(source, input)) : Optional.empty();
        }
        catch (TrinoException e) {
            if (!CastPreimages.conversionFailure(e)) {
                throw e;
            }
            return Optional.empty();
        }
        catch (IllegalArgumentException e) {
            if (!(target instanceof TimestampWithTimeZoneType) || !e.getMessage().startsWith("Millis overflow:")) {
                throw e;
            }
            return Optional.empty();
        }
    }
}
