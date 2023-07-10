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
package io.trino.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import io.airlift.slice.Slice;
import io.trino.json.CachingResolver.ResolvedOperatorAndCoercions;
import io.trino.json.JsonPathEvaluator.Invoker;
import io.trino.json.ir.IrAbsMethod;
import io.trino.json.ir.IrArithmeticBinary;
import io.trino.json.ir.IrArithmeticUnary;
import io.trino.json.ir.IrArrayAccessor;
import io.trino.json.ir.IrCeilingMethod;
import io.trino.json.ir.IrConstantJsonSequence;
import io.trino.json.ir.IrContextVariable;
import io.trino.json.ir.IrDatetimeMethod;
import io.trino.json.ir.IrDescendantMemberAccessor;
import io.trino.json.ir.IrDoubleMethod;
import io.trino.json.ir.IrFilter;
import io.trino.json.ir.IrFloorMethod;
import io.trino.json.ir.IrJsonNull;
import io.trino.json.ir.IrJsonPathVisitor;
import io.trino.json.ir.IrKeyValueMethod;
import io.trino.json.ir.IrLastIndexVariable;
import io.trino.json.ir.IrLiteral;
import io.trino.json.ir.IrMemberAccessor;
import io.trino.json.ir.IrNamedJsonVariable;
import io.trino.json.ir.IrNamedValueVariable;
import io.trino.json.ir.IrPathNode;
import io.trino.json.ir.IrPredicateCurrentItemVariable;
import io.trino.json.ir.IrSizeMethod;
import io.trino.json.ir.IrTypeMethod;
import io.trino.json.ir.SqlJsonLiteralConverter;
import io.trino.json.ir.TypedValue;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.CachingResolver.ResolvedOperatorAndCoercions.RESOLUTION_ERROR;
import static io.trino.json.JsonEmptySequenceNode.EMPTY_SEQUENCE;
import static io.trino.json.PathEvaluationError.itemTypeError;
import static io.trino.json.PathEvaluationError.structuralError;
import static io.trino.json.PathEvaluationUtil.unwrapArrays;
import static io.trino.json.ir.IrArithmeticUnary.Sign.PLUS;
import static io.trino.json.ir.SqlJsonLiteralConverter.getTextTypedValue;
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
import static io.trino.operator.scalar.MathFunctions.ceilingFloat;
import static io.trino.operator.scalar.MathFunctions.floorFloat;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.DecimalCasts.longDecimalToDouble;
import static io.trino.type.DecimalCasts.shortDecimalToDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class PathEvaluationVisitor
        extends IrJsonPathVisitor<List<Object>, PathEvaluationContext>
{
    private final boolean lax;
    private final JsonNode input;
    private final Object[] parameters;
    private final PathPredicateEvaluationVisitor predicateVisitor;
    private final Invoker invoker;
    private final CachingResolver resolver;
    private int objectId;

    public PathEvaluationVisitor(boolean lax, JsonNode input, Object[] parameters, Invoker invoker, CachingResolver resolver)
    {
        this.lax = lax;
        this.input = requireNonNull(input, "input is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.resolver = requireNonNull(resolver, "resolver is null");
        this.predicateVisitor = new PathPredicateEvaluationVisitor(lax, this, invoker, resolver);
    }

    @Override
    protected List<Object> visitIrPathNode(IrPathNode node, PathEvaluationContext context)
    {
        throw new UnsupportedOperationException("JSON path evaluating visitor not implemented for " + node.getClass().getSimpleName());
    }

    @Override
    protected List<Object> visitIrAbsMethod(IrAbsMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            TypedValue value;
            if (object instanceof JsonNode) {
                value = getNumericTypedValue((JsonNode) object)
                        .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) object).getNodeType().name()));
            }
            else {
                value = (TypedValue) object;
            }
            outputSequence.add(getAbsoluteValue(value));
        }

        return outputSequence.build();
    }

    private static TypedValue getAbsoluteValue(TypedValue typedValue)
    {
        Type type = typedValue.getType();

        if (type.equals(BIGINT)) {
            long value = typedValue.getLongValue();
            if (value >= 0) {
                return typedValue;
            }
            long absValue;
            try {
                absValue = abs(value);
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, absValue);
        }
        if (type.equals(INTEGER)) {
            long value = typedValue.getLongValue();
            if (value >= 0) {
                return typedValue;
            }
            long absValue;
            try {
                absValue = absInteger(value);
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, absValue);
        }
        if (type.equals(SMALLINT)) {
            long value = typedValue.getLongValue();
            if (value >= 0) {
                return typedValue;
            }
            long absValue;
            try {
                absValue = absSmallint(value);
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, absValue);
        }
        if (type.equals(TINYINT)) {
            long value = typedValue.getLongValue();
            if (value >= 0) {
                return typedValue;
            }
            long absValue;
            try {
                absValue = absTinyint(value);
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, absValue);
        }
        if (type.equals(DOUBLE)) {
            double value = typedValue.getDoubleValue();
            if (value >= 0) {
                return typedValue;
            }
            return new TypedValue(type, abs(value));
        }
        if (type.equals(REAL)) {
            float value = intBitsToFloat((int) typedValue.getLongValue());
            if (value > 0) {
                return typedValue;
            }
            return new TypedValue(type, floatToRawIntBits(Math.abs(value)));
        }
        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                long value = typedValue.getLongValue();
                if (value > 0) {
                    return typedValue;
                }
                return new TypedValue(type, -value);
            }
            Int128 value = (Int128) typedValue.getObjectValue();
            if (value.isNegative()) {
                Int128 result;
                try {
                    result = DecimalOperators.Negation.negate((Int128) typedValue.getObjectValue());
                }
                catch (Exception e) {
                    throw new PathEvaluationError(e);
                }
                return new TypedValue(type, result);
            }
            return typedValue;
        }

        throw itemTypeError("NUMBER", type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrArithmeticBinary(IrArithmeticBinary node, PathEvaluationContext context)
    {
        List<Object> leftSequence = process(node.left(), context);
        List<Object> rightSequence = process(node.right(), context);

        if (lax) {
            leftSequence = unwrapArrays(leftSequence);
            rightSequence = unwrapArrays(rightSequence);
        }

        if (leftSequence.size() != 1 || rightSequence.size() != 1) {
            throw new PathEvaluationError("arithmetic binary expression requires singleton operands");
        }

        TypedValue left;
        Object leftObject = getOnlyElement(leftSequence);
        if (leftObject instanceof JsonNode) {
            left = getNumericTypedValue((JsonNode) leftObject)
                    .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) leftObject).getNodeType().name()));
        }
        else {
            left = (TypedValue) leftObject;
        }

        TypedValue right;
        Object rightObject = getOnlyElement(rightSequence);
        if (rightObject instanceof JsonNode) {
            right = getNumericTypedValue((JsonNode) rightObject)
                    .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) rightObject).getNodeType().name()));
        }
        else {
            right = (TypedValue) rightObject;
        }

        ResolvedOperatorAndCoercions operators = resolver.getOperators(node, OperatorType.valueOf(node.operator().name()), left.getType(), right.getType());
        if (operators == RESOLUTION_ERROR) {
            throw new PathEvaluationError(format("invalid operand types to %s operator (%s, %s)", node.operator().name(), left.getType(), right.getType()));
        }

        Object leftInput = left.getValueAsObject();
        if (operators.getLeftCoercion().isPresent()) {
            try {
                leftInput = invoker.invoke(operators.getLeftCoercion().get(), ImmutableList.of(leftInput));
            }
            catch (RuntimeException e) {
                throw new PathEvaluationError(e);
            }
        }

        Object rightInput = right.getValueAsObject();
        if (operators.getRightCoercion().isPresent()) {
            try {
                rightInput = invoker.invoke(operators.getRightCoercion().get(), ImmutableList.of(rightInput));
            }
            catch (RuntimeException e) {
                throw new PathEvaluationError(e);
            }
        }

        Object result;
        try {
            result = invoker.invoke(operators.getOperator(), ImmutableList.of(leftInput, rightInput));
        }
        catch (RuntimeException e) {
            throw new PathEvaluationError(e);
        }

        return ImmutableList.of(TypedValue.fromValueAsObject(operators.getOperator().getSignature().getReturnType(), result));
    }

    @Override
    protected List<Object> visitIrArithmeticUnary(IrArithmeticUnary node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            TypedValue value;
            if (object instanceof JsonNode) {
                value = getNumericTypedValue((JsonNode) object)
                        .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) object).getNodeType().name()));
            }
            else {
                value = (TypedValue) object;
                Type type = value.getType();
                if (!type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT) && !type.equals(DOUBLE) && !type.equals(REAL) && !(type instanceof DecimalType)) {
                    throw itemTypeError("NUMBER", type.getDisplayName());
                }
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
        Type type = typedValue.getType();

        if (type.equals(BIGINT)) {
            long negatedValue;
            try {
                negatedValue = BigintOperators.negate(typedValue.getLongValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, negatedValue);
        }
        if (type.equals(INTEGER)) {
            long negatedValue;
            try {
                negatedValue = IntegerOperators.negate(typedValue.getLongValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, negatedValue);
        }
        if (type.equals(SMALLINT)) {
            long negatedValue;
            try {
                negatedValue = SmallintOperators.negate(typedValue.getLongValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, negatedValue);
        }
        if (type.equals(TINYINT)) {
            long negatedValue;
            try {
                negatedValue = TinyintOperators.negate(typedValue.getLongValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, negatedValue);
        }
        if (type.equals(DOUBLE)) {
            return new TypedValue(type, -typedValue.getDoubleValue());
        }
        if (type.equals(REAL)) {
            return new TypedValue(type, RealOperators.negate(typedValue.getLongValue()));
        }
        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                return new TypedValue(type, -typedValue.getLongValue());
            }
            Int128 negatedValue;
            try {
                negatedValue = DecimalOperators.Negation.negate((Int128) typedValue.getObjectValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
            return new TypedValue(type, negatedValue);
        }

        throw new IllegalStateException("unexpected type" + type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrArrayAccessor(IrArrayAccessor node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            List<Object> elements;
            if (object instanceof JsonNode) {
                if (((JsonNode) object).isArray()) {
                    elements = ImmutableList.copyOf(((JsonNode) object).elements());
                }
                else if (lax) {
                    elements = ImmutableList.of((object));
                }
                else {
                    throw itemTypeError("ARRAY", ((JsonNode) object).getNodeType().name());
                }
            }
            else if (lax) {
                elements = ImmutableList.of((object));
            }
            else {
                throw itemTypeError("ARRAY", ((TypedValue) object).getType().getDisplayName());
            }

            // handle wildcard accessor
            if (node.subscripts().isEmpty()) {
                outputSequence.addAll(elements);
                continue;
            }

            if (elements.isEmpty()) {
                if (!lax) {
                    throw structuralError("invalid array subscript for empty array");
                }
                // for lax mode, the result is empty sequence
                continue;
            }

            PathEvaluationContext arrayContext = context.withLast(elements.size() - 1);
            for (IrArrayAccessor.Subscript subscript : node.subscripts()) {
                List<Object> from = process(subscript.from(), arrayContext);
                Optional<List<Object>> to = subscript.to().map(path -> process(path, arrayContext));
                if (from.size() != 1) {
                    throw new PathEvaluationError("array subscript 'from' value must be singleton numeric");
                }
                if (to.isPresent() && to.get().size() != 1) {
                    throw new PathEvaluationError("array subscript 'to' value must be singleton numeric");
                }
                long fromIndex = asArrayIndex(getOnlyElement(from));
                long toIndex = to
                        .map(Iterables::getOnlyElement)
                        .map(PathEvaluationVisitor::asArrayIndex)
                        .orElse(fromIndex);

                if (!lax && (fromIndex < 0 || fromIndex >= elements.size() || toIndex < 0 || toIndex >= elements.size() || fromIndex > toIndex)) {
                    throw structuralError("invalid array subscript: [%s, %s] for array of size %s", fromIndex, toIndex, elements.size());
                }

                if (fromIndex <= toIndex) {
                    Range<Long> allElementsRange = Range.closed(0L, (long) elements.size() - 1);
                    Range<Long> subscriptRange = Range.closed(fromIndex, toIndex);
                    if (subscriptRange.isConnected(allElementsRange)) { // cannot intersect ranges which are not connected...
                        Range<Long> resultRange = subscriptRange.intersection(allElementsRange);
                        if (!resultRange.isEmpty()) {
                            for (long i = resultRange.lowerEndpoint(); i <= resultRange.upperEndpoint(); i++) {
                                outputSequence.add(elements.get((int) i));
                            }
                        }
                    }
                }
            }
        }

        return outputSequence.build();
    }

    private static long asArrayIndex(Object object)
    {
        if (object instanceof JsonNode jsonNode) {
            if (jsonNode.getNodeType() != JsonNodeType.NUMBER) {
                throw itemTypeError("NUMBER", (jsonNode.getNodeType().name()));
            }
            if (!jsonNode.canConvertToLong()) {
                throw new PathEvaluationError(format("cannot convert value %s to long", jsonNode));
            }
            return jsonNode.longValue();
        }
        TypedValue value = (TypedValue) object;
        Type type = value.getType();
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return value.getLongValue();
        }
        if (type.equals(DOUBLE)) {
            try {
                return DoubleOperators.castToLong(value.getDoubleValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }
        if (type.equals(REAL)) {
            try {
                return RealOperators.castToLong(value.getLongValue());
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }
        if (type instanceof DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            if (((DecimalType) type).isShort()) {
                long tenToScale = longTenToNth(DecimalConversions.intScale(scale));
                return DecimalCasts.shortDecimalToBigint(value.getLongValue(), precision, scale, tenToScale);
            }
            Int128 tenToScale = Int128Math.powerOfTen(DecimalConversions.intScale(scale));
            try {
                return DecimalCasts.longDecimalToBigint((Int128) value.getObjectValue(), precision, scale, tenToScale);
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }

        throw itemTypeError("NUMBER", type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrCeilingMethod(IrCeilingMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            TypedValue value;
            if (object instanceof JsonNode) {
                value = getNumericTypedValue((JsonNode) object)
                        .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) object).getNodeType().name()));
            }
            else {
                value = (TypedValue) object;
            }
            outputSequence.add(getCeiling(value));
        }

        return outputSequence.build();
    }

    private static TypedValue getCeiling(TypedValue typedValue)
    {
        Type type = typedValue.getType();

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return typedValue;
        }
        if (type.equals(DOUBLE)) {
            return new TypedValue(type, Math.ceil(typedValue.getDoubleValue()));
        }
        if (type.equals(REAL)) {
            return new TypedValue(type, ceilingFloat(typedValue.getLongValue()));
        }
        if (type instanceof DecimalType decimalType) {
            int scale = decimalType.getScale();
            DecimalType resultType = DecimalType.createDecimalType(decimalType.getPrecision() - scale + Math.min(scale, 1), 0);
            if (decimalType.isShort()) {
                return new TypedValue(resultType, ceilingShort(scale, typedValue.getLongValue()));
            }
            if (resultType.isShort()) {
                try {
                    return new TypedValue(resultType, ceilingLongShort(scale, (Int128) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationError(e);
                }
            }
            try {
                return new TypedValue(resultType, ceilingLong(scale, (Int128) typedValue.getObjectValue()));
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }

        throw itemTypeError("NUMBER", type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrConstantJsonSequence(IrConstantJsonSequence node, PathEvaluationContext context)
    {
        return ImmutableList.copyOf(node.sequence());
    }

    @Override
    protected List<Object> visitIrContextVariable(IrContextVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(input);
    }

    @Override
    protected List<Object> visitIrDatetimeMethod(IrDatetimeMethod node, PathEvaluationContext context) // TODO
    {
        throw new UnsupportedOperationException("date method is not yet supported");
    }

    @Override
    protected List<Object> visitIrDescendantMemberAccessor(IrDescendantMemberAccessor node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        sequence.stream()
                .forEach(object -> descendants(object, node.key(), builder));

        return builder.build();
    }

    private void descendants(Object object, String key, ImmutableList.Builder<Object> builder)
    {
        if (object instanceof JsonNode jsonNode && jsonNode.isObject()) {
            // prefix order: visit the enclosing object first
            JsonNode boundValue = jsonNode.get(key);
            if (boundValue != null) {
                builder.add(boundValue);
            }
            // recurse into child nodes
            ImmutableList.copyOf(jsonNode.fields()).stream()
                    .forEach(field -> descendants(field.getValue(), key, builder));
        }
        if (object instanceof JsonNode jsonNode && jsonNode.isArray()) {
            for (int index = 0; index < jsonNode.size(); index++) {
                descendants(jsonNode.get(index), key, builder);
            }
        }
    }

    @Override
    protected List<Object> visitIrDoubleMethod(IrDoubleMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            TypedValue value;
            if (object instanceof JsonNode) {
                value = getNumericTypedValue((JsonNode) object)
                        .orElseGet(() -> getTextTypedValue((JsonNode) object)
                                .orElseThrow(() -> itemTypeError("NUMBER or TEXT", ((JsonNode) object).getNodeType().name())));
            }
            else {
                value = (TypedValue) object;
            }
            outputSequence.add(getDouble(value));
        }

        return outputSequence.build();
    }

    private static TypedValue getDouble(TypedValue typedValue)
    {
        Type type = typedValue.getType();

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return new TypedValue(DOUBLE, (double) typedValue.getLongValue());
        }
        if (type.equals(DOUBLE)) {
            return typedValue;
        }
        if (type.equals(REAL)) {
            return new TypedValue(DOUBLE, RealOperators.castToDouble(typedValue.getLongValue()));
        }
        if (type instanceof DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            if (((DecimalType) type).isShort()) {
                long tenToScale = longTenToNth(DecimalConversions.intScale(scale));
                return new TypedValue(DOUBLE, shortDecimalToDouble(typedValue.getLongValue(), precision, scale, tenToScale));
            }
            Int128 tenToScale = Int128Math.powerOfTen(DecimalConversions.intScale(scale));
            return new TypedValue(DOUBLE, longDecimalToDouble((Int128) typedValue.getObjectValue(), precision, scale, tenToScale));
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            try {
                return new TypedValue(DOUBLE, VarcharOperators.castToDouble((Slice) typedValue.getObjectValue()));
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }

        throw itemTypeError("NUMBER or TEXT", type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrFilter(IrFilter node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            PathEvaluationContext currentItemContext = context.withCurrentItem(object);
            Boolean result = predicateVisitor.process(node.predicate(), currentItemContext);
            if (Boolean.TRUE.equals(result)) {
                outputSequence.add(object);
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Object> visitIrFloorMethod(IrFloorMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            TypedValue value;
            if (object instanceof JsonNode) {
                value = getNumericTypedValue((JsonNode) object)
                        .orElseThrow(() -> itemTypeError("NUMBER", ((JsonNode) object).getNodeType().name()));
            }
            else {
                value = (TypedValue) object;
            }
            outputSequence.add(getFloor(value));
        }

        return outputSequence.build();
    }

    private static TypedValue getFloor(TypedValue typedValue)
    {
        Type type = typedValue.getType();

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return typedValue;
        }
        if (type.equals(DOUBLE)) {
            return new TypedValue(type, Math.floor(typedValue.getDoubleValue()));
        }
        if (type.equals(REAL)) {
            return new TypedValue(type, floorFloat(typedValue.getLongValue()));
        }
        if (type instanceof DecimalType decimalType) {
            int scale = decimalType.getScale();
            DecimalType resultType = DecimalType.createDecimalType(decimalType.getPrecision() - scale + Math.min(scale, 1), 0);
            if (((DecimalType) type).isShort()) {
                return new TypedValue(resultType, floorShort(scale, typedValue.getLongValue()));
            }
            if (resultType.isShort()) {
                try {
                    return new TypedValue(resultType, floorLongShort(scale, (Int128) typedValue.getObjectValue()));
                }
                catch (Exception e) {
                    throw new PathEvaluationError(e);
                }
            }
            try {
                return new TypedValue(resultType, floorLong(scale, (Int128) typedValue.getObjectValue()));
            }
            catch (Exception e) {
                throw new PathEvaluationError(e);
            }
        }

        throw itemTypeError("NUMBER", type.getDisplayName());
    }

    @Override
    protected List<Object> visitIrJsonNull(IrJsonNull node, PathEvaluationContext context)
    {
        return ImmutableList.of(NullNode.getInstance());
    }

    @Override
    protected List<Object> visitIrKeyValueMethod(IrKeyValueMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            if (!(object instanceof JsonNode)) {
                throw itemTypeError("OBJECT", ((TypedValue) object).getType().getDisplayName());
            }
            if (!((JsonNode) object).isObject()) {
                throw itemTypeError("OBJECT", ((JsonNode) object).getNodeType().name());
            }

            // non-unique keys are not supported. if they were, we should follow the spec here on handling them.
            // see the comment in `visitIrMemberAccessor` method.
            ((JsonNode) object).fields().forEachRemaining(
                    field -> outputSequence.add(new ObjectNode(
                            JsonNodeFactory.instance,
                            ImmutableMap.of(
                                    "name", TextNode.valueOf(field.getKey()),
                                    "value", field.getValue(),
                                    "id", IntNode.valueOf(objectId)))));
            objectId++;
        }

        return outputSequence.build();
    }

    @Override
    protected List<Object> visitIrLastIndexVariable(IrLastIndexVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(context.getLast());
    }

    @Override
    protected List<Object> visitIrLiteral(IrLiteral node, PathEvaluationContext context)
    {
        return ImmutableList.of(TypedValue.fromValueAsObject(node.type().orElseThrow(), node.value()));
    }

    @Override
    protected List<Object> visitIrMemberAccessor(IrMemberAccessor node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        if (lax) {
            sequence = unwrapArrays(sequence);
        }

        // due to the choice of JsonNode as JSON representation, there cannot be duplicate keys in a JSON object.
        // according to the spec, it is implementation-dependent whether non-unique keys are allowed.
        // in case when there are duplicate keys, the spec describes the way of handling them both
        // by the wildcard member accessor and by the 'by-key' member accessor.
        // this method needs to be revisited when switching to another JSON representation.
        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            if (!lax) {
                if (!(object instanceof JsonNode jsonNode)) {
                    throw itemTypeError("OBJECT", ((TypedValue) object).getType().getDisplayName());
                }
                if (!jsonNode.isObject()) {
                    throw itemTypeError("OBJECT", jsonNode.getNodeType().name());
                }
            }

            if (object instanceof JsonNode jsonNode && jsonNode.isObject()) {
                // handle wildcard member accessor
                if (node.key().isEmpty()) {
                    outputSequence.addAll(jsonNode.elements());
                }
                else {
                    JsonNode boundValue = jsonNode.get(node.key().get());
                    if (boundValue == null) {
                        if (!lax) {
                            throw structuralError("missing member '%s' in JSON object", node.key().get());
                        }
                    }
                    else {
                        outputSequence.add(boundValue);
                    }
                }
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Object> visitIrNamedJsonVariable(IrNamedJsonVariable node, PathEvaluationContext context)
    {
        Object value = parameters[node.index()];
        checkState(value != null, "missing value for parameter");
        checkState(value instanceof JsonNode, "expected JSON, got SQL value");

        if (value.equals(EMPTY_SEQUENCE)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(value);
    }

    @Override
    protected List<Object> visitIrNamedValueVariable(IrNamedValueVariable node, PathEvaluationContext context)
    {
        Object value = parameters[node.index()];
        checkState(value != null, "missing value for parameter");
        checkState(value instanceof TypedValue || value instanceof NullNode, "expected SQL value or JSON null, got non-null JSON");

        return ImmutableList.of(value);
    }

    @Override
    protected List<Object> visitIrPredicateCurrentItemVariable(IrPredicateCurrentItemVariable node, PathEvaluationContext context)
    {
        return ImmutableList.of(context.getCurrentItem());
    }

    @Override
    protected List<Object> visitIrSizeMethod(IrSizeMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();
        for (Object object : sequence) {
            if (object instanceof JsonNode && ((JsonNode) object).isArray()) {
                outputSequence.add(new TypedValue(INTEGER, ((JsonNode) object).size()));
            }
            else {
                if (lax) {
                    outputSequence.add(new TypedValue(INTEGER, 1));
                }
                else {
                    String type;
                    if (object instanceof JsonNode) {
                        type = ((JsonNode) object).getNodeType().name();
                    }
                    else {
                        type = ((TypedValue) object).getType().getDisplayName();
                    }
                    throw itemTypeError("ARRAY", type);
                }
            }
        }

        return outputSequence.build();
    }

    @Override
    protected List<Object> visitIrTypeMethod(IrTypeMethod node, PathEvaluationContext context)
    {
        List<Object> sequence = process(node.base(), context);

        Type resultType = node.type().orElseThrow();
        ImmutableList.Builder<Object> outputSequence = ImmutableList.builder();

        // In case when a new type is supported in JSON path, it might be necessary to update the
        // constant JsonPathAnalyzer.TYPE_METHOD_RESULT_TYPE, which determines the resultType.
        // Today it is only enough to fit the longest of the result strings below.
        for (Object object : sequence) {
            if (object instanceof JsonNode) {
                switch (((JsonNode) object).getNodeType()) {
                    case NUMBER:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("number")));
                        break;
                    case STRING:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("string")));
                        break;
                    case BOOLEAN:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("boolean")));
                        break;
                    case ARRAY:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("array")));
                        break;
                    case OBJECT:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("object")));
                        break;
                    case NULL:
                        outputSequence.add(new TypedValue(resultType, utf8Slice("null")));
                        break;
                    default:
                        throw new IllegalArgumentException("unexpected Json node type: " + ((JsonNode) object).getNodeType());
                }
            }
            else {
                Type type = ((TypedValue) object).getType();
                if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) || type.equals(DOUBLE) || type.equals(REAL) || type instanceof DecimalType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("number")));
                }
                else if (type instanceof VarcharType || type instanceof CharType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("string")));
                }
                else if (type.equals(BOOLEAN)) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("boolean")));
                }
                else if (type.equals(DATE)) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("date")));
                }
                else if (type instanceof TimeType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("time without time zone")));
                }
                else if (type instanceof TimeWithTimeZoneType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("time with time zone")));
                }
                else if (type instanceof TimestampType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("timestamp without time zone")));
                }
                else if (type instanceof TimestampWithTimeZoneType) {
                    outputSequence.add(new TypedValue(resultType, utf8Slice("timestamp with time zone")));
                }
            }
        }

        return outputSequence.build();
    }

    private static Optional<TypedValue> getNumericTypedValue(JsonNode jsonNode)
    {
        try {
            return SqlJsonLiteralConverter.getNumericTypedValue(jsonNode);
        }
        catch (SqlJsonLiteralConverter.JsonLiteralConversionError e) {
            throw new PathEvaluationError(e);
        }
    }
}
