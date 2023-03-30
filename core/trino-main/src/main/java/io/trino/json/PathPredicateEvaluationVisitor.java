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
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.json.CachingResolver.ResolvedOperatorAndCoercions;
import io.trino.json.JsonPathEvaluator.Invoker;
import io.trino.json.ir.IrComparisonPredicate;
import io.trino.json.ir.IrConjunctionPredicate;
import io.trino.json.ir.IrDisjunctionPredicate;
import io.trino.json.ir.IrExistsPredicate;
import io.trino.json.ir.IrIsUnknownPredicate;
import io.trino.json.ir.IrJsonPathVisitor;
import io.trino.json.ir.IrNegationPredicate;
import io.trino.json.ir.IrPathNode;
import io.trino.json.ir.IrPredicate;
import io.trino.json.ir.IrStartsWithPredicate;
import io.trino.json.ir.SqlJsonLiteralConverter;
import io.trino.json.ir.TypedValue;
import io.trino.operator.scalar.StringFunctions;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ComparisonExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.json.CachingResolver.ResolvedOperatorAndCoercions.RESOLUTION_ERROR;
import static io.trino.json.PathEvaluationUtil.unwrapArrays;
import static io.trino.json.ir.IrComparisonPredicate.Operator.EQUAL;
import static io.trino.json.ir.IrComparisonPredicate.Operator.NOT_EQUAL;
import static io.trino.json.ir.SqlJsonLiteralConverter.getTextTypedValue;
import static io.trino.json.ir.SqlJsonLiteralConverter.getTypedValue;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isCharacterStringType;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

/**
 * This visitor evaluates the JSON path predicate in JSON filter expression.
 * The returned value is true, false or unknown.
 * <p>
 * Filter predicate never throws error, both in lax or strict mode, even if evaluation
 * of the nested JSON path fails or the predicate itself cannot be successfully evaluated
 * (e.g. because it tries to compare incompatible types).
 * <p>
 * NOTE Even though errors are suppressed both in lax and strict mode, the mode affects
 * the predicate result.
 * For example, let `$array` be a JSON array of size 3: `["a", "b", "c"]`,
 * and let predicate be `exists($array[5])`
 * The nested accessor expression `$array[5]` is a structural error (array index out of bounds).
 * In lax mode, the error is suppressed, and `$array[5]` results in an empty sequence.
 * Hence, the `exists` predicate returns `false`.
 * In strict mode, the error is not suppressed, and the `exists` predicate returns `unknown`.
 * <p>
 * NOTE on the semantics of comparison:
 * The following comparison operators are supported in JSON path predicate: EQUAL, NOT EQUAL, LESS THAN, GREATER THAN, LESS THAN OR EQUAL, GREATER THAN OR EQUAL.
 * Both operands are JSON paths, and so they are evaluated to sequences of objects.
 * <p>
 * Technically, each of the objects is either a JsonNode, or a TypedValue.
 * Logically, they can be divided into three categories:
 * 1. scalar values. These are all the TypedValues and certain subtypes of JsonNode, e.g. IntNode, BooleanNode,...
 * 2. non-scalars. These are JSON arrays and objects
 * 3. NULL values. They are represented by JsonNode subtype NullNode.
 * <p>
 * When comparing two objects, the following rules apply:
 * 1. NULL can be successfully compared with any object. NULL equals NULL, and is neither equal, less than or greater than any other object.
 * 2. non-scalars can be only compared with a NULL (the result being false). Comparing a non-scalar with any other object (including itself) results in error.
 * 3. scalars can be compared with a NULL (the result being false). They can be also compared with other scalars, provided that the types of the
 * compared scalars are eligible for comparison. Otherwise, comparing two scalars results in error.
 * <p>
 * As mentioned before, the operands to comparison predicate produce sequences of objects.
 * Comparing the sequences requires comparing every pair of objects from the first and the second sequence.
 * The overall result of the comparison predicate depends on two factors:
 * - if any comparison resulted in error,
 * - if any comparison returned true.
 * In strict mode, any error makes the overall result unknown.
 * In lax mode, the SQL specification allows to either ignore errors, or return unknown in case of error.
 * Our implementation choice is to finish the predicate evaluation as early as possible, that is,
 * to return unknown on the first error or return true on the first comparison returning true.
 * The result is deterministic, because the input sequences are processed in order.
 * In case of no errors, the comparison predicate result is whether any comparison returned true.
 * <p>
 * NOTE The starts with predicate, similarly to the comparison predicate, is applied to sequences of input items.
 * It applies the same policy of translating errors into unknown result, and the same policy of returning true
 * on the first success.
 */
class PathPredicateEvaluationVisitor
        extends IrJsonPathVisitor<Boolean, PathEvaluationContext>
{
    private final boolean lax;
    private final PathEvaluationVisitor pathVisitor;
    private final Invoker invoker;
    private final CachingResolver resolver;

    public PathPredicateEvaluationVisitor(boolean lax, PathEvaluationVisitor pathVisitor, Invoker invoker, CachingResolver resolver)
    {
        this.lax = lax;
        this.pathVisitor = requireNonNull(pathVisitor, "pathVisitor is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.resolver = requireNonNull(resolver, "resolver is null");
    }

    @Override
    protected Boolean visitIrPathNode(IrPathNode node, PathEvaluationContext context)
    {
        throw new IllegalStateException("JSON predicate evaluating visitor applied to a non-predicate node " + node.getClass().getSimpleName());
    }

    @Override
    protected Boolean visitIrPredicate(IrPredicate node, PathEvaluationContext context)
    {
        throw new UnsupportedOperationException("JSON predicate evaluating visitor not implemented for " + node.getClass().getSimpleName());
    }

    @Override
    protected Boolean visitIrComparisonPredicate(IrComparisonPredicate node, PathEvaluationContext context)
    {
        List<Object> leftSequence;
        try {
            leftSequence = pathVisitor.process(node.getLeft(), context);
        }
        catch (PathEvaluationError e) {
            return null;
        }

        List<Object> rightSequence;
        try {
            rightSequence = pathVisitor.process(node.getRight(), context);
        }
        catch (PathEvaluationError e) {
            return null;
        }

        if (lax) {
            leftSequence = unwrapArrays(leftSequence);
            rightSequence = unwrapArrays(rightSequence);
        }

        if (leftSequence.isEmpty() || rightSequence.isEmpty()) {
            return FALSE;
        }

        boolean leftHasJsonNull = false;
        boolean leftHasScalar = false;
        boolean leftHasNonScalar = false;
        for (Object object : leftSequence) {
            if (object instanceof JsonNode) {
                if (object instanceof NullNode) {
                    leftHasJsonNull = true;
                }
                else if (((JsonNode) object).isValueNode()) {
                    leftHasScalar = true;
                }
                else {
                    leftHasNonScalar = true;
                }
            }
            else {
                leftHasScalar = true;
            }
        }

        boolean rightHasJsonNull = false;
        boolean rightHasScalar = false;
        boolean rightHasNonScalar = false;
        for (Object object : rightSequence) {
            if (object instanceof JsonNode) {
                if (((JsonNode) object).isNull()) {
                    rightHasJsonNull = true;
                }
                else if (((JsonNode) object).isValueNode()) {
                    rightHasScalar = true;
                }
                else {
                    rightHasNonScalar = true;
                }
            }
            else {
                rightHasScalar = true;
            }
        }

        // try to find a quick error, i.e. a pair of left and right items which are of non-comparable categories
        if (leftHasNonScalar && rightHasNonScalar ||
                leftHasNonScalar && rightHasScalar ||
                leftHasScalar && rightHasNonScalar) {
            return null;
        }

        boolean found = false;

        // try to find a quick null-based answer for == and <> operators
        if (node.getOperator() == EQUAL && leftHasJsonNull && rightHasJsonNull) {
            found = true;
        }
        if (node.getOperator() == NOT_EQUAL) {
            if (leftHasJsonNull && (rightHasScalar || rightHasNonScalar) ||
                    rightHasJsonNull && (leftHasScalar || leftHasNonScalar)) {
                found = true;
            }
        }
        if (found && lax) {
            return TRUE;
        }

        // compare scalars from left and right sequence
        if (!leftHasScalar || !rightHasScalar) {
            return found;
        }
        List<TypedValue> leftScalars = getScalars(leftSequence);
        if (leftScalars == null) {
            return null;
        }
        List<TypedValue> rightScalars = getScalars(rightSequence);
        if (rightScalars == null) {
            return null;
        }
        for (TypedValue leftValue : leftScalars) {
            for (TypedValue rightValue : rightScalars) {
                Boolean result = compare(node, leftValue, rightValue);
                if (result == null) {
                    return null;
                }
                if (TRUE.equals(result)) {
                    found = true;
                    if (lax) {
                        return TRUE;
                    }
                }
            }
        }

        return found;
    }

    private Boolean compare(IrComparisonPredicate node, TypedValue left, TypedValue right)
    {
        IrComparisonPredicate.Operator comparisonOperator = node.getOperator();
        ComparisonExpression.Operator operator;
        Type firstType = left.getType();
        Object firstValue = left.getValueAsObject();
        Type secondType = right.getType();
        Object secondValue = right.getValueAsObject();
        switch (comparisonOperator) {
            case EQUAL:
            case NOT_EQUAL:
                operator = ComparisonExpression.Operator.EQUAL;
                break;
            case LESS_THAN:
                operator = ComparisonExpression.Operator.LESS_THAN;
                break;
            case GREATER_THAN:
                operator = ComparisonExpression.Operator.LESS_THAN;
                firstType = right.getType();
                firstValue = right.getValueAsObject();
                secondType = left.getType();
                secondValue = left.getValueAsObject();
                break;
            case LESS_THAN_OR_EQUAL:
                operator = ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN_OR_EQUAL:
                operator = ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
                firstType = right.getType();
                firstValue = right.getValueAsObject();
                secondType = left.getType();
                secondValue = left.getValueAsObject();
                break;
            default:
                throw new UnsupportedOperationException("Unexpected comparison operator " + comparisonOperator);
        }

        ResolvedOperatorAndCoercions operators = resolver.getOperators(node, OperatorType.valueOf(operator.name()), firstType, secondType);
        if (operators == RESOLUTION_ERROR) {
            return null;
        }

        if (operators.getLeftCoercion().isPresent()) {
            try {
                firstValue = invoker.invoke(operators.getLeftCoercion().get(), ImmutableList.of(firstValue));
            }
            catch (RuntimeException e) {
                return null;
            }
        }

        if (operators.getRightCoercion().isPresent()) {
            try {
                secondValue = invoker.invoke(operators.getRightCoercion().get(), ImmutableList.of(secondValue));
            }
            catch (RuntimeException e) {
                return null;
            }
        }

        Object result;
        try {
            result = invoker.invoke(operators.getOperator(), ImmutableList.of(firstValue, secondValue));
        }
        catch (RuntimeException e) {
            return null;
        }

        if (comparisonOperator == NOT_EQUAL) {
            return !(Boolean) result;
        }
        return (Boolean) result;
    }

    @Override
    protected Boolean visitIrConjunctionPredicate(IrConjunctionPredicate node, PathEvaluationContext context)
    {
        Boolean left = process(node.getLeft(), context);
        if (FALSE.equals(left)) {
            return FALSE;
        }
        Boolean right = process(node.getRight(), context);
        if (FALSE.equals(right)) {
            return FALSE;
        }
        if (left == null || right == null) {
            return null;
        }
        return TRUE;
    }

    @Override
    protected Boolean visitIrDisjunctionPredicate(IrDisjunctionPredicate node, PathEvaluationContext context)
    {
        Boolean left = process(node.getLeft(), context);
        if (TRUE.equals(left)) {
            return TRUE;
        }
        Boolean right = process(node.getRight(), context);
        if (TRUE.equals(right)) {
            return TRUE;
        }
        if (left == null || right == null) {
            return null;
        }
        return FALSE;
    }

    @Override
    protected Boolean visitIrExistsPredicate(IrExistsPredicate node, PathEvaluationContext context)
    {
        List<Object> sequence;
        try {
            sequence = pathVisitor.process(node.getPath(), context);
        }
        catch (PathEvaluationError e) {
            return null;
        }

        return !sequence.isEmpty();
    }

    @Override
    protected Boolean visitIrIsUnknownPredicate(IrIsUnknownPredicate node, PathEvaluationContext context)
    {
        Boolean predicateResult = process(node.getPredicate(), context);

        return predicateResult == null;
    }

    @Override
    protected Boolean visitIrNegationPredicate(IrNegationPredicate node, PathEvaluationContext context)
    {
        Boolean predicateResult = process(node.getPredicate(), context);

        return predicateResult == null ? null : !predicateResult;
    }

    @Override
    protected Boolean visitIrStartsWithPredicate(IrStartsWithPredicate node, PathEvaluationContext context)
    {
        List<Object> valueSequence;
        try {
            valueSequence = pathVisitor.process(node.getValue(), context);
        }
        catch (PathEvaluationError e) {
            return null;
        }

        List<Object> prefixSequence;
        try {
            prefixSequence = pathVisitor.process(node.getPrefix(), context);
        }
        catch (PathEvaluationError e) {
            return null;
        }
        if (prefixSequence.size() != 1) {
            return null;
        }
        Slice prefix = getText(getOnlyElement(prefixSequence));
        if (prefix == null) {
            return null;
        }

        if (lax) {
            valueSequence = unwrapArrays(valueSequence);
        }
        if (valueSequence.isEmpty()) {
            return FALSE;
        }

        boolean found = false;
        for (Object object : valueSequence) {
            Slice value = getText(object);
            if (value == null) {
                return null;
            }
            if (StringFunctions.startsWith(value, prefix)) {
                found = true;
                if (lax) {
                    return TRUE;
                }
            }
        }

        return found;
    }

    private static List<TypedValue> getScalars(List<Object> sequence)
    {
        ImmutableList.Builder<TypedValue> scalars = ImmutableList.builder();
        for (Object object : sequence) {
            if (object instanceof TypedValue) {
                scalars.add((TypedValue) object);
            }
            else {
                JsonNode jsonNode = (JsonNode) object;
                if (jsonNode.isValueNode() && !jsonNode.isNull()) {
                    Optional<TypedValue> typedValue;
                    try {
                        typedValue = getTypedValue(jsonNode);
                    }
                    catch (SqlJsonLiteralConverter.JsonLiteralConversionError e) {
                        return null;
                    }
                    if (typedValue.isEmpty()) {
                        return null;
                    }
                    scalars.add(typedValue.get());
                }
            }
        }

        return scalars.build();
    }

    private static Slice getText(Object object)
    {
        if (object instanceof TypedValue typedValue) {
            if (isCharacterStringType(typedValue.getType())) {
                if (typedValue.getType() instanceof CharType) {
                    return padSpaces((Slice) typedValue.getObjectValue(), (CharType) typedValue.getType());
                }
                return (Slice) typedValue.getObjectValue();
            }
            return null;
        }
        JsonNode jsonNode = (JsonNode) object;
        return getTextTypedValue(jsonNode)
                .map(TypedValue::getObjectValue)
                .map(Slice.class::cast)
                .orElse(null);
    }
}
