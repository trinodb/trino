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
package io.trino.jsonpath;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
import io.trino.jsonpath.CachingResolver.ResolvedOperatorAndCoercions;
import io.trino.jsonpath.JsonPathEvaluator.Invoker;
import io.trino.jsonpath.ir.IrAbsMethod;
import io.trino.jsonpath.ir.IrArithmeticBinary;
import io.trino.jsonpath.ir.IrArithmeticUnary;
import io.trino.jsonpath.ir.IrArrayAccessor;
import io.trino.jsonpath.ir.IrCeilingMethod;
import io.trino.jsonpath.ir.IrConstantJsonSequence;
import io.trino.jsonpath.ir.IrContextVariable;
import io.trino.jsonpath.ir.IrDatetimeMethod;
import io.trino.jsonpath.ir.IrDescendantMemberAccessor;
import io.trino.jsonpath.ir.IrDoubleMethod;
import io.trino.jsonpath.ir.IrFilter;
import io.trino.jsonpath.ir.IrFloorMethod;
import io.trino.jsonpath.ir.IrJsonNull;
import io.trino.jsonpath.ir.IrJsonPathVisitor;
import io.trino.jsonpath.ir.IrKeyValueMethod;
import io.trino.jsonpath.ir.IrLastIndexVariable;
import io.trino.jsonpath.ir.IrLiteral;
import io.trino.jsonpath.ir.IrMemberAccessor;
import io.trino.jsonpath.ir.IrNamedJsonVariable;
import io.trino.jsonpath.ir.IrNamedValueVariable;
import io.trino.jsonpath.ir.IrPathNode;
import io.trino.jsonpath.ir.IrPredicateCurrentItemVariable;
import io.trino.jsonpath.ir.IrSizeMethod;
import io.trino.jsonpath.ir.IrTypeMethod;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.BigintOperators;
import io.trino.type.DecimalCasts;
import io.trino.type.DecimalOperators;
import io.trino.type.DoubleOperators;
import io.trino.type.IntegerOperators;
import io.trino.type.RealOperators;
import io.trino.type.SmallintOperators;
import io.trino.type.TinyintOperators;
import io.trino.type.VarcharOperators;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jsonpath.CachingResolver.ResolvedOperatorAndCoercions.RESOLUTION_ERROR;
import static io.trino.jsonpath.PathEvaluationException.itemTypeError;
import static io.trino.jsonpath.PathEvaluationException.structuralError;
import static io.trino.jsonpath.PathEvaluationUtil.unwrapArrays;
import static io.trino.jsonpath.ir.IrArithmeticUnary.Sign.PLUS;
import static io.trino.jsonpath.ir.SqlJsonLiteralConverter.getNumericTypedValue;
import static io.trino.jsonpath.ir.SqlJsonLiteralConverter.getTextTypedValue;
import static io.trino.operator.scalar.MathFunctions.Ceiling.ceilingLong;
import static io.trino.operator.scalar.MathFunctions.Ceiling.ceilingLongShort;
import static io.trino.operator.scalar.MathFunctions.Ceiling.ceilingShort;
import static io.trino.operator.scalar.MathFunctions.Floor.floorLong;
import static io.trino.operator.scalar.MathFunctions.Floor.floorLongShort;
import static io.trino.operator.scalar.MathFunctions.Floor.floorShort;
import static io.trino.operator.scalar.MathFunctions.abs;
import static io.trino.operator.scalar.MathFunctions.absInteger;
import static io.trino.operator.scalar.MathFunctions.absSmallint;
import static io.trino.operator.scalar.MathFunctions.absTinyint;
import static io.trino.operator.scalar.MathFunctions.ceilingReal;
import static io.trino.operator.scalar.MathFunctions.floorReal;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isCharacterStringType;
import static io.trino.type.DateTimes.extractTimePrecision;
import static io.trino.type.DateTimes.extractTimestampPrecision;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.DateTimes.timeHasTimeZone;
import static io.trino.type.DateTimes.timestampHasTimeZone;
import static io.trino.type.DecimalCasts.longDecimalToDouble;
import static io.trino.type.DecimalCasts.shortDecimalToDouble;
import static io.trino.util.DateTimeUtils.parseDate;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class PathEvaluationVisitor
        extends IrJsonPathVisitor<List<Json>, PathEvaluationContext>
{
    private final boolean lax;
    private final Json input;
    private final Object[] parameters;
    private final PathPredicateEvaluationVisitor predicateVisitor;
    private final Invoker invoker;
    private final CachingResolver resolver;
    private int objectId;

    public PathEvaluationVisitor(boolean lax, Json input, Object[] parameters, Invoker invoker, CachingResolver resolver)
    {
        this.lax = lax;
        this.input = requireNonNull(input, "input is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.predicateVisitor = new PathPredicateEvaluationVisitor(lax, this, invoker, resolver);
    }

    @Override
    protected List<Json> visitIrPathNode(IrPathNode node, PathEvaluationContext context)
    {
        throw new UnsupportedOperationException("JSON path evaluating visitor not implemented for " + node.getClass().getSimpleName());
    }

    @Override
    protected List<Json> visitIrAbsMethod(IrAbsMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            outputSequence.add(getAbsoluteValue(asNumericTypedValue(item, "NUMBER")));
        }

        return outputSequence.build();
    }

    private static TypedValue getAbsoluteValue(TypedValue typedValue)
    {
        Type type = typedValue.type();
        return switch (type) {
            case BigintType _ -> absLong(typedValue, type, value -> abs(value));
            case IntegerType _ -> absLong(typedValue, type, value -> absInteger(value));
            case SmallintType _ -> absLong(typedValue, type, value -> absSmallint(value));
            case TinyintType _ -> absLong(typedValue, type, value -> absTinyint(value));
            case DoubleType _ -> {
                double value = typedValue.getDoubleValue();
                yield value >= 0 ? typedValue : new TypedValue(type, abs(value));
            }
            case RealType _ -> {
                float value = intBitsToFloat((int) typedValue.getLongValue());
                yield value > 0 ? typedValue : new TypedValue(type, floatToRawIntBits(Math.abs(value)));
            }
            case DecimalType decimalType -> {
                if (decimalType.isShort()) {
                    long value = typedValue.getLongValue();
                    yield value > 0 ? typedValue : new TypedValue(type, -value);
                }
                Int128 value = (Int128) typedValue.getObjectValue();
                if (!value.isNegative()) {
                    yield typedValue;
                }
                try {
                    yield new TypedValue(type, DecimalOperators.Negation.negate(value));
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case NumberType _ -> {
                TrinoNumber number = (TrinoNumber) typedValue.getObjectValue();
                yield new TypedValue(type, switch (number.toBigDecimal()) {
                    case TrinoNumber.NotANumber _ -> number;
                    case TrinoNumber.Infinity _ -> TrinoNumber.from(new TrinoNumber.Infinity(false));
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> TrinoNumber.from(decimal.abs());
                });
            }
            default -> throw itemTypeError("NUMBER", type.getDisplayName());
        };
    }

    private static TypedValue absLong(TypedValue typedValue, Type type, LongUnaryOperator absFunction)
    {
        long value = typedValue.getLongValue();
        if (value >= 0) {
            return typedValue;
        }
        try {
            return new TypedValue(type, absFunction.applyAsLong(value));
        }
        catch (Exception e) {
            throw new PathEvaluationException(e);
        }
    }

    @Override
    protected List<Json> visitIrArithmeticBinary(IrArithmeticBinary node, PathEvaluationContext context)
    {
        List<Json> leftSequence = process(node.left(), context);
        List<Json> rightSequence = process(node.right(), context);

        if (lax) {
            leftSequence = unwrapArrays(leftSequence);
            rightSequence = unwrapArrays(rightSequence);
        }

        if (leftSequence.size() != 1 || rightSequence.size() != 1) {
            throw new PathEvaluationException("arithmetic binary expression requires singleton operands");
        }

        TypedValue left = asNumericTypedValue(getOnlyElement(leftSequence), "NUMBER");
        TypedValue right = asNumericTypedValue(getOnlyElement(rightSequence), "NUMBER");

        ResolvedOperatorAndCoercions operators = resolver.getOperators(node, OperatorType.valueOf(node.operator().name()), left.type(), right.type());
        if (operators == RESOLUTION_ERROR) {
            throw new PathEvaluationException(format("invalid operand types to %s operator (%s, %s)", node.operator().name(), left.type(), right.type()));
        }

        Object leftInput = left.value();
        if (operators.getLeftCoercion().isPresent()) {
            try {
                leftInput = invoker.invoke(operators.getLeftCoercion().get(), ImmutableList.of(leftInput));
            }
            catch (RuntimeException e) {
                throw new PathEvaluationException(e);
            }
        }

        Object rightInput = right.value();
        if (operators.getRightCoercion().isPresent()) {
            try {
                rightInput = invoker.invoke(operators.getRightCoercion().get(), ImmutableList.of(rightInput));
            }
            catch (RuntimeException e) {
                throw new PathEvaluationException(e);
            }
        }

        Object result;
        try {
            result = invoker.invoke(operators.getOperator(), ImmutableList.of(leftInput, rightInput));
        }
        catch (RuntimeException e) {
            throw new PathEvaluationException(e);
        }

        return ImmutableList.of(TypedValue.fromValueAsObject(operators.getOperator().signature().getReturnType(), result));
    }

    @Override
    protected List<Json> visitIrArithmeticUnary(IrArithmeticUnary node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            TypedValue value;
            if (item instanceof TypedValue typed) {
                value = typed;
                if (!isNumericType(value.type())) {
                    throw itemTypeError("NUMBER", value.type().getDisplayName());
                }
            }
            else {
                Json json = (Json) item;
                value = getNumericTypedValue(json)
                        .orElseThrow(() -> itemTypeError("NUMBER", itemTypeName(json)));
            }
            if (node.sign() == PLUS) {
                outputSequence.add(value);
            }
            else {
                outputSequence.add(negate(value));
            }
        }

        return outputSequence.build();
    }

    private static TypedValue negate(TypedValue typedValue)
    {
        Type type = typedValue.type();
        return switch (type) {
            case BigintType _ -> negateLong(typedValue, type, value -> BigintOperators.negate(value));
            case IntegerType _ -> negateLong(typedValue, type, value -> IntegerOperators.negate(value));
            case SmallintType _ -> negateLong(typedValue, type, value -> SmallintOperators.negate(value));
            case TinyintType _ -> negateLong(typedValue, type, value -> TinyintOperators.negate(value));
            case DoubleType _ -> new TypedValue(type, -typedValue.getDoubleValue());
            case RealType _ -> new TypedValue(type, RealOperators.negate(typedValue.getLongValue()));
            case DecimalType decimalType -> {
                if (decimalType.isShort()) {
                    yield new TypedValue(type, -typedValue.getLongValue());
                }
                try {
                    yield new TypedValue(type, DecimalOperators.Negation.negate((Int128) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case NumberType _ -> {
                TrinoNumber number = (TrinoNumber) typedValue.getObjectValue();
                yield new TypedValue(type, switch (number.toBigDecimal()) {
                    case TrinoNumber.NotANumber _ -> number;
                    case TrinoNumber.Infinity(boolean negative) -> TrinoNumber.from(new TrinoNumber.Infinity(!negative));
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> TrinoNumber.from(decimal.negate());
                });
            }
            default -> throw new IllegalStateException("unexpected type " + type.getDisplayName());
        };
    }

    private static TypedValue negateLong(TypedValue typedValue, Type type, LongUnaryOperator negateFunction)
    {
        try {
            return new TypedValue(type, negateFunction.applyAsLong(typedValue.getLongValue()));
        }
        catch (Exception e) {
            throw new PathEvaluationException(e);
        }
    }

    @Override
    protected List<Json> visitIrArrayAccessor(IrArrayAccessor node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            // Determine the array source: typed JSON view or lax-mode wrap. Each supplies
            // a size and an int-indexed element accessor so subscript evaluation can fetch
            // only the indices it needs, without materializing the entire element list.
            int size;
            IntFunction<Json> elementAt;
            Json arrayJson = null;
            if (item.isArray()) {
                arrayJson = item;
                size = item.arraySize();
                elementAt = item::arrayElement;
            }
            else if (lax) {
                Json fixed = item;
                size = 1;
                elementAt = _ -> fixed;
            }
            else {
                throw itemTypeError("ARRAY", itemTypeName(item));
            }

            // Wildcard accessor: emit every element. For typed JSON, forEachArrayElement
            // is a single linear walk; for the lax-mode wrap there is exactly one element.
            if (node.subscripts().isEmpty()) {
                if (arrayJson != null) {
                    arrayJson.forEachArrayElement(outputSequence::add);
                }
                else {
                    outputSequence.add(elementAt.apply(0));
                }
                continue;
            }

            if (size == 0) {
                if (!lax) {
                    throw structuralError("invalid array subscript for empty array");
                }
                // for lax mode, the result is empty sequence
                continue;
            }

            PathEvaluationContext arrayContext = context.withLast(size - 1);
            for (IrArrayAccessor.Subscript subscript : node.subscripts()) {
                List<Json> from = process(subscript.from(), arrayContext);
                Optional<List<Json>> to = subscript.to().map(path -> process(path, arrayContext));
                if (from.size() != 1) {
                    throw new PathEvaluationException("array subscript 'from' value must be singleton numeric");
                }
                if (to.isPresent() && to.get().size() != 1) {
                    throw new PathEvaluationException("array subscript 'to' value must be singleton numeric");
                }
                long fromIndex = asArrayIndex(getOnlyElement(from));
                long toIndex = to
                        .map(Iterables::getOnlyElement)
                        .map(PathEvaluationVisitor::asArrayIndex)
                        .orElse(fromIndex);

                if (!lax && (fromIndex < 0 || fromIndex >= size || toIndex < 0 || toIndex >= size || fromIndex > toIndex)) {
                    throw structuralError("invalid array subscript: [%s, %s] for array of size %s", fromIndex, toIndex, size);
                }

                if (fromIndex <= toIndex) {
                    Range<Long> allElementsRange = Range.closed(0L, (long) size - 1);
                    Range<Long> subscriptRange = Range.closed(fromIndex, toIndex);
                    if (subscriptRange.isConnected(allElementsRange)) { // cannot intersect ranges which are not connected...
                        Range<Long> resultRange = subscriptRange.intersection(allElementsRange);
                        if (!resultRange.isEmpty()) {
                            for (long i = resultRange.lowerEndpoint(); i <= resultRange.upperEndpoint(); i++) {
                                outputSequence.add(elementAt.apply((int) i));
                            }
                        }
                    }
                }
            }
        }

        return outputSequence.build();
    }

    private static long asArrayIndex(Json item)
    {
        TypedValue value;
        if (item instanceof TypedValue typed) {
            value = typed;
        }
        else {
            Json json = (Json) item;
            value = getNumericTypedValue(json)
                    .orElseThrow(() -> itemTypeError("NUMBER", itemTypeName(json)));
        }
        Type type = value.type();
        return switch (type) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> value.getLongValue();
            case DoubleType _ -> {
                try {
                    yield DoubleOperators.castToBigint(value.getDoubleValue());
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case RealType _ -> {
                try {
                    yield RealOperators.castToBigint(value.getLongValue());
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case DecimalType decimalType -> {
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (decimalType.isShort()) {
                    long tenToScale = longTenToNth(DecimalConversions.intScale(scale));
                    yield DecimalCasts.shortDecimalToBigint(value.getLongValue(), precision, scale, tenToScale);
                }
                Int128 tenToScale = Int128Math.powerOfTen(DecimalConversions.intScale(scale));
                try {
                    yield DecimalCasts.longDecimalToBigint((Int128) value.getObjectValue(), precision, scale, tenToScale);
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            default -> throw itemTypeError("NUMBER", type.getDisplayName());
        };
    }

    @Override
    protected List<Json> visitIrCeilingMethod(IrCeilingMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            outputSequence.add(getCeiling(asNumericTypedValue(item, "NUMBER")));
        }

        return outputSequence.build();
    }

    private static TypedValue getCeiling(TypedValue typedValue)
    {
        Type type = typedValue.type();
        return switch (type) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> typedValue;
            case DoubleType _ -> new TypedValue(type, Math.ceil(typedValue.getDoubleValue()));
            case RealType _ -> new TypedValue(type, ceilingReal(typedValue.getLongValue()));
            case DecimalType decimalType -> {
                int scale = decimalType.getScale();
                DecimalType resultType = DecimalType.createDecimalType(decimalType.getPrecision() - scale + Math.min(scale, 1), 0);
                if (decimalType.isShort()) {
                    yield new TypedValue(resultType, ceilingShort(scale, typedValue.getLongValue()));
                }
                try {
                    yield new TypedValue(resultType,
                            resultType.isShort()
                                    ? ceilingLongShort(scale, (Int128) typedValue.getObjectValue())
                                    : ceilingLong(scale, (Int128) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case NumberType _ -> {
                TrinoNumber number = (TrinoNumber) typedValue.getObjectValue();
                yield new TypedValue(type, switch (number.toBigDecimal()) {
                    case TrinoNumber.NotANumber _ -> number;
                    case TrinoNumber.Infinity _ -> number;
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> TrinoNumber.from(decimal.setScale(0, RoundingMode.CEILING));
                });
            }
            default -> throw itemTypeError("NUMBER", type.getDisplayName());
        };
    }

    @Override
    protected List<Json> visitIrConstantJsonSequence(IrConstantJsonSequence node, PathEvaluationContext context)
    {
        ImmutableList.Builder<Json> builder = ImmutableList.builder();
        for (JsonNode item : node.sequence()) {
            builder.add(JsonItems.fromJsonNode(item));
        }
        return builder.build();
    }

    @Override
    protected List<Json> visitIrContextVariable(IrContextVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(input);
    }

    @Override
    protected List<Json> visitIrDatetimeMethod(IrDatetimeMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);
        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            TypedValue text = textOf(item);
            try {
                outputSequence.add(node.format()
                        .map(template -> template.parseValue(((Slice) text.getObjectValue()).toStringUtf8()))
                        .orElseGet(() -> getDatetime(text)));
            }
            catch (RuntimeException e) {
                // A value that cannot be parsed must surface as a path evaluation error, so that
                // the ON ERROR clause of the enclosing function applies.
                throw new PathEvaluationException(e);
            }
        }

        return outputSequence.build();
    }

    private static TypedValue textOf(Json item)
    {
        if (item instanceof TypedValue typed) {
            if (!isCharacterStringType(typed.type())) {
                throw itemTypeError("TEXT", typed.type().getDisplayName());
            }
            return typed;
        }
        Json json = (Json) item;
        return getTextTypedValue(json)
                .orElseThrow(() -> itemTypeError("TEXT", itemTypeName(json)));
    }

    private static TypedValue getDatetime(TypedValue typedValue)
    {
        Slice slice = (Slice) typedValue.getObjectValue();

        // The standard datetime shapes are unambiguously distinguishable by their separators:
        // a TIMESTAMP value has a space between date and time, a TIME value has at least one
        // colon, and a DATE value has neither. Dispatch on shape so each input passes through
        // the matching parser exactly once. Only the timestamp and time parsers need the value as a
        // String, so a date never decodes one.
        if (slice.indexOfByte(' ') >= 0) {
            String value = slice.toStringUtf8();
            int precision = extractTimestampPrecision(value);
            if (timestampHasTimeZone(value)) {
                return TypedValue.fromValueAsObject(createTimestampWithTimeZoneType(precision), parseTimestampWithTimeZone(precision, value));
            }
            return TypedValue.fromValueAsObject(createTimestampType(precision), parseTimestamp(precision, value));
        }
        if (slice.indexOfByte(':') >= 0) {
            String value = slice.toStringUtf8();
            int precision = extractTimePrecision(value);
            if (timeHasTimeZone(value)) {
                return TypedValue.fromValueAsObject(createTimeWithTimeZoneType(precision), parseTimeWithTimeZone(precision, value));
            }
            return new TypedValue(createTimeType(precision), parseTime(value));
        }
        return new TypedValue(DATE, parseDate(slice));
    }

    private static String getText(TypedValue typedValue)
    {
        Type type = typedValue.type();
        if (type instanceof CharType charType) {
            return Chars.padSpaces((Slice) typedValue.getObjectValue(), charType).toStringUtf8();
        }
        if (type instanceof VarcharType) {
            return ((Slice) typedValue.getObjectValue()).toStringUtf8();
        }
        throw itemTypeError("TEXT", type.getDisplayName());
    }

    @Override
    protected List<Json> visitIrDescendantMemberAccessor(IrDescendantMemberAccessor node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        ImmutableList.Builder<Json> builder = ImmutableList.builder();
        sequence.stream()
                .forEach(item -> descendants(item, node.key(), builder));

        return builder.build();
    }

    private void descendants(Json item, String key, ImmutableList.Builder<Json> builder)
    {
        if (!(item instanceof Json json)) {
            return;
        }
        if (json.isObject()) {
            // prefix order: visit the enclosing object first
            json.objectMember(key).ifPresent(builder::add);
            // recurse into child nodes
            json.forEachObjectMember((_, value) -> descendants(value, key, builder));
        }
        else if (json.isArray()) {
            json.forEachArrayElement(elem -> descendants(elem, key, builder));
        }
    }

    @Override
    protected List<Json> visitIrDoubleMethod(IrDoubleMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            TypedValue value;
            if (item instanceof TypedValue typed) {
                value = typed;
            }
            else {
                Json json = (Json) item;
                value = getNumericTypedValue(json)
                        .orElseGet(() -> getTextTypedValue(json)
                                .orElseThrow(() -> itemTypeError("NUMBER or TEXT", itemTypeName(json))));
            }
            outputSequence.add(getDouble(value));
        }

        return outputSequence.build();
    }

    private static TypedValue getDouble(TypedValue typedValue)
    {
        Type type = typedValue.type();
        return switch (type) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> new TypedValue(DOUBLE, (double) typedValue.getLongValue());
            case DoubleType _ -> typedValue;
            case RealType _ -> new TypedValue(DOUBLE, RealOperators.castToDouble(typedValue.getLongValue()));
            case DecimalType decimalType -> {
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (decimalType.isShort()) {
                    long tenToScale = longTenToNth(DecimalConversions.intScale(scale));
                    yield new TypedValue(DOUBLE, shortDecimalToDouble(typedValue.getLongValue(), precision, scale, tenToScale));
                }
                Int128 tenToScale = Int128Math.powerOfTen(DecimalConversions.intScale(scale));
                yield new TypedValue(DOUBLE, longDecimalToDouble((Int128) typedValue.getObjectValue(), precision, scale, tenToScale));
            }
            case VarcharType _, CharType _ -> {
                try {
                    yield new TypedValue(DOUBLE, VarcharOperators.castToDouble((Slice) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case NumberType _ -> {
                TrinoNumber number = (TrinoNumber) typedValue.getObjectValue();
                yield new TypedValue(DOUBLE, switch (number.toBigDecimal()) {
                    case TrinoNumber.NotANumber _ -> Double.NaN;
                    case TrinoNumber.Infinity(boolean negative) -> negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> decimal.doubleValue();
                });
            }
            default -> throw itemTypeError("NUMBER or TEXT", type.getDisplayName());
        };
    }

    @Override
    protected List<Json> visitIrFilter(IrFilter node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            PathEvaluationContext currentItemContext = context.withCurrentItem(item);
            Boolean result = predicateVisitor.process(node.predicate(), currentItemContext);
            if (Boolean.TRUE.equals(result)) {
                outputSequence.add(item);
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Json> visitIrFloorMethod(IrFloorMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            outputSequence.add(getFloor(asNumericTypedValue(item, "NUMBER")));
        }

        return outputSequence.build();
    }

    private static TypedValue getFloor(TypedValue typedValue)
    {
        Type type = typedValue.type();
        return switch (type) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> typedValue;
            case DoubleType _ -> new TypedValue(type, Math.floor(typedValue.getDoubleValue()));
            case RealType _ -> new TypedValue(type, floorReal(typedValue.getLongValue()));
            case DecimalType decimalType -> {
                int scale = decimalType.getScale();
                DecimalType resultType = DecimalType.createDecimalType(decimalType.getPrecision() - scale + Math.min(scale, 1), 0);
                if (decimalType.isShort()) {
                    yield new TypedValue(resultType, floorShort(scale, typedValue.getLongValue()));
                }
                try {
                    yield new TypedValue(resultType,
                            resultType.isShort()
                                    ? floorLongShort(scale, (Int128) typedValue.getObjectValue())
                                    : floorLong(scale, (Int128) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationException(e);
                }
            }
            case NumberType _ -> {
                TrinoNumber number = (TrinoNumber) typedValue.getObjectValue();
                yield new TypedValue(type, switch (number.toBigDecimal()) {
                    case TrinoNumber.NotANumber _ -> number;
                    case TrinoNumber.Infinity _ -> number;
                    case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> TrinoNumber.from(decimal.setScale(0, RoundingMode.FLOOR));
                });
            }
            default -> throw itemTypeError("NUMBER", type.getDisplayName());
        };
    }

    @Override
    protected List<Json> visitIrJsonNull(IrJsonNull node, PathEvaluationContext context)
    {
        return ImmutableList.of(JsonItemBuilder.JSON_NULL);
    }

    @Override
    protected List<Json> visitIrKeyValueMethod(IrKeyValueMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            if (!item.isObject()) {
                throw itemTypeError("OBJECT", itemTypeName(item));
            }

            // forEachObjectMember yields every entry in insertion order, so duplicate keys
            // (SQL:2023 §9.42 WITHOUT UNIQUE KEYS) emit one keyvalue() row per occurrence —
            // the spec's behavior for the wildcard form over non-unique keys.
            int currentObjectId = objectId;
            item.forEachObjectMember((name, value) -> outputSequence.add(JsonItemBuilder.encode(w -> w.startObject()
                    .fieldName("name").varchar(utf8Slice(name))
                    .fieldName("value").nest(value)
                    .fieldName("id").integerValue(currentObjectId)
                    .endObject())));
            objectId++;
        }

        return outputSequence.build();
    }

    @Override
    protected List<Json> visitIrLastIndexVariable(IrLastIndexVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(context.getLast());
    }

    @Override
    protected List<Json> visitIrLiteral(IrLiteral node, PathEvaluationContext context)
    {
        return ImmutableList.of(TypedValue.fromValueAsObject(node.type().orElseThrow(), node.value()));
    }

    @Override
    protected List<Json> visitIrMemberAccessor(IrMemberAccessor node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        // SQL:2023 §9.46: under WITHOUT UNIQUE KEYS the wildcard accessor yields every
        // member in insertion order, and the by-key accessor yields every member whose
        // key matches (again in insertion order). Json#objectMembers exposes the plural
        // form; iterate it directly so duplicate keys aren't silently dropped.
        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            if (!lax) {
                if (!item.isObject()) {
                    throw itemTypeError("OBJECT", itemTypeName(item));
                }
            }

            if (item instanceof Json json && json.isObject()) {
                if (node.key().isEmpty()) {
                    json.forEachObjectMember((_, value) -> outputSequence.add(value));
                }
                else {
                    int[] matches = {0};
                    json.objectMembers(node.key().get(), value -> {
                        outputSequence.add(value);
                        matches[0]++;
                    });
                    if (matches[0] == 0 && !lax) {
                        throw structuralError("missing member '%s' in JSON object", node.key().get());
                    }
                }
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Json> visitIrNamedJsonVariable(IrNamedJsonVariable node, PathEvaluationContext context)
    {
        Object value = parameters[node.index()];
        if (value == null) {
            // null FORMAT-JSON parameter produces an empty sequence
            return ImmutableList.of();
        }
        // TypedValue now implements Json — guard against it explicitly so an SQL scalar
        // bound to an `IrNamedJsonVariable` slot is still rejected here.
        checkState(value instanceof Json && !(value instanceof TypedValue), "expected JSON, got SQL value");
        return ImmutableList.of((Json) value);
    }

    @Override
    protected List<Json> visitIrNamedValueVariable(IrNamedValueVariable node, PathEvaluationContext context)
    {
        Object value = parameters[node.index()];
        checkState(value != null, "missing value for parameter");
        checkState(value instanceof TypedValue || (value instanceof Json json && json.isNull()),
                "expected SQL value or JSON null, got non-null JSON");

        return ImmutableList.of((Json) value);
    }

    @Override
    protected List<Json> visitIrPredicateCurrentItemVariable(IrPredicateCurrentItemVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(context.getCurrentItem());
    }

    @Override
    protected List<Json> visitIrSizeMethod(IrSizeMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();
        for (Json item : sequence) {
            if (item.isArray()) {
                outputSequence.add(new TypedValue(INTEGER, item.arraySize()));
            }
            else {
                if (lax) {
                    outputSequence.add(new TypedValue(INTEGER, 1));
                }
                else {
                    throw itemTypeError("ARRAY", itemTypeName(item));
                }
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Json> visitIrTypeMethod(IrTypeMethod node, PathEvaluationContext context)
    {
        List<Json> sequence = process(node.base(), context);

        Type resultType = node.type().orElseThrow();
        ImmutableList.Builder<Json> outputSequence = ImmutableList.builder();

        // In case when a new type is supported in JSON path, it might be necessary to update the
        // constant JsonPathAnalyzer.TYPE_METHOD_RESULT_TYPE, which determines the resultType.
        // Today it is only enough to fit the longest of the result strings below.
        for (Json item : sequence) {
            // TypedValue must be checked first: it implements Json but carries SQL-side
            // types (DATE, TIME, etc.) that the Json-side kind/scalarType vocabulary can't
            // express.
            if (item instanceof TypedValue typed) {
                Type type = typed.type();
                String name = switch (type) {
                    case BigintType _, IntegerType _, SmallintType _, TinyintType _,
                         DoubleType _, RealType _, DecimalType _, NumberType _ -> "number";
                    case VarcharType _, CharType _ -> "string";
                    case BooleanType _ -> "boolean";
                    case DateType _ -> "date";
                    case TimeType _ -> "time without time zone";
                    case TimeWithTimeZoneType _ -> "time with time zone";
                    case TimestampType _ -> "timestamp without time zone";
                    case TimestampWithTimeZoneType _ -> "timestamp with time zone";
                    default -> null;
                };
                if (name != null) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice(name)));
                }
            }
            else {
                Json json = (Json) item;
                String name = switch (json.kind()) {
                    case ARRAY -> "array";
                    case OBJECT -> "object";
                    case NULL -> "null";
                    case SCALAR -> switch (json.scalarType()) {
                        case BOOLEAN -> "boolean";
                        case VARCHAR -> "string";
                        case BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, DECIMAL, NUMBER -> "number";
                    };
                    case ERROR -> throw new IllegalStateException("unexpected JSON_ERROR in path result");
                };
                outputSequence.add(new TypedValue(resultType, utf8Slice(name)));
            }
        }

        return outputSequence.build();
    }

    /// The type name a value is reported as in `itemTypeError` messages: the JSON kind for a
    /// JSON value, and the SQL type's display name for a scalar carrying one.
    static String itemTypeName(Json json)
    {
        // TypedValue first: it carries SQL-side types whose display name we want verbatim,
        // not the JSON kind/scalar-tag translation.
        if (json instanceof TypedValue typed) {
            return typed.type().getDisplayName();
        }
        return switch (json.kind()) {
            case ARRAY -> "ARRAY";
            case OBJECT -> "OBJECT";
            case NULL -> "NULL";
            case ERROR -> "JSON_ERROR";
            case SCALAR -> scalarTypeName(json.scalarType());
        };
    }

    private static String scalarTypeName(TypeTag tag)
    {
        return switch (tag) {
            case BOOLEAN -> "BOOLEAN";
            case VARCHAR -> "STRING";
            case BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, DECIMAL, NUMBER -> "NUMBER";
        };
    }

    private static TypedValue asNumericTypedValue(Json item, String expected)
    {
        // TypedValue passes through verbatim — SQL scalars carry their type
        // already; only the Json branch needs the encoded-scalar lookup.
        if (item instanceof TypedValue typed) {
            return typed;
        }
        Json json = (Json) item;
        return getNumericTypedValue(json)
                .orElseThrow(() -> itemTypeError(expected, itemTypeName(json)));
    }

    private static boolean isNumericType(Type type)
    {
        return switch (type) {
            case BigintType _, IntegerType _, SmallintType _, TinyintType _,
                 DoubleType _, RealType _, DecimalType _, NumberType _ -> true;
            default -> false;
        };
    }
}
