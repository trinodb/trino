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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.ArraySubscriptOperator;
import io.trino.operator.scalar.FormatFunction;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.Coercer;
import io.trino.sql.planner.LiteralInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolResolver;
import io.trino.sql.planner.TranslationMap;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.ComparisonExpression.Operator;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import io.trino.type.FunctionType;
import io.trino.type.LikeFunctions;
import io.trino.type.LikePattern;
import io.trino.type.TypeCoercion;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.type.RowType.anonymous;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.tree.DereferenceExpression.isQualifiedAllFieldsReference;
import static java.lang.Math.toIntExact;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ExpressionInterpreter
{
    private final Expression expression;
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions;
    private final LiteralInterpreter literalInterpreter;
    private final ConnectorSession connectorSession;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final InterpretedFunctionInvoker functionInvoker;
    private final TypeCoercion typeCoercion;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, LikePattern> likePatternCache = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();

    public ExpressionInterpreter(Expression expression, PlannerContext plannerContext, Session session, Map<NodeRef<Expression>, Type> expressionTypes, Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.resolvedFunctions = requireNonNull(resolvedFunctions, "resolvedFunctions is null");
        this.literalInterpreter = new LiteralInterpreter(plannerContext, session);
        this.connectorSession = session.toConnectorSession();
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        verify(expressionTypes.containsKey(NodeRef.of(expression)));
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
    }

    public static Object evaluateConstantExpression(
            Expression expression,
            Type expectedType,
            PlannerContext plannerContext,
            Session session,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Analysis analysis = new Analysis(null, ImmutableMap.of(), QueryType.OTHERS);
        Scope scope = Scope.create();
        ExpressionAnalyzer.analyzeExpressionWithoutSubqueries(
                session,
                plannerContext,
                accessControl,
                scope,
                analysis,
                expression,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                WarningCollector.NOOP,
                CorrelationSupport.DISALLOWED);

        // Apply casts, desugar expression, and preform other rewrites
        TranslationMap translationMap = new TranslationMap(Optional.empty(), scope, analysis, ImmutableMap.of(), ImmutableList.of(), session, plannerContext);
        expression = coerceIfNecessary(analysis, expression, translationMap.rewrite(expression));

        // The expression tree has been rewritten which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        ExpressionAnalyzer analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(expression, scope);

        Type actualType = analyzer.getExpressionTypes().get(NodeRef.of(expression));
        if (!new TypeCoercion(plannerContext.getTypeManager()::getType).canCoerce(actualType, expectedType)) {
            throw semanticException(TYPE_MISMATCH, expression, "Cannot cast type %s to %s", actualType.getDisplayName(), expectedType.getDisplayName());
        }

        Map<NodeRef<Expression>, Type> coercions = ImmutableMap.<NodeRef<Expression>, Type>builder()
                .putAll(analyzer.getExpressionCoercions())
                .put(NodeRef.of(expression), expectedType)
                .buildOrThrow();

        // add coercions
        Expression rewrite = Coercer.addCoercions(expression, coercions);

        // redo the analysis since above expression rewriter might create new expressions which do not have entries in the type map
        analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(rewrite, Scope.create());

        // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(rewrite, Scope.create());

        // expressionInterpreter/optimizer only understands a subset of expression types
        // TODO: remove this when the new expression tree is implemented
        Expression canonicalized = canonicalizeExpression(rewrite, analyzer.getExpressionTypes(), plannerContext, session);

        // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(canonicalized, Scope.create());

        // evaluate the expression
        return new ExpressionInterpreter(canonicalized, plannerContext, session, analyzer.getExpressionTypes(), analyzer.getResolvedFunctions()).evaluate();
    }

    private static Expression coerceIfNecessary(Analysis analysis, Expression original, Expression rewritten)
    {
        Type coercion = analysis.getCoercion(original);
        if (coercion == null) {
            return rewritten;
        }

        return new Cast(rewritten, toSqlType(coercion), false);
    }

    private Object evaluate()
    {
        Object result = new Visitor().processWithExceptionHandling(expression, null);
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    private class Visitor
            extends AstVisitor<Object, Object>
    {
        private Object processWithExceptionHandling(Expression expression, Object context)
        {
            if (expression == null) {
                return null;
            }

            Object result = process(expression, context);
            verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
            return result;
        }

        @Override
        public Object visitFieldReference(FieldReference node, Object context)
        {
            throw new UnsupportedOperationException("Field references not supported in interpreter");
        }

        @Override
        protected Object visitDereferenceExpression(DereferenceExpression node, Object context)
        {
            checkArgument(!isQualifiedAllFieldsReference(node), "unexpected expression: all fields labeled reference %s", node);
            Identifier fieldIdentifier = node.getField().orElseThrow();

            // Row dereference: process dereference base eagerly, and only then pick the expected field
            Object base = processWithExceptionHandling(node.getBase(), context);
            // if the base part is evaluated to be null, the dereference expression should also be null
            if (base == null) {
                return null;
            }

            Type type = type(node.getBase());
            RowType rowType = (RowType) type;
            SqlRow row = (SqlRow) base;
            Type returnType = type(node);
            String fieldName = fieldIdentifier.getValue();
            List<Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            checkState(index >= 0, "could not find field name: %s", fieldName);
            return readNativeValue(returnType, row.getRawFieldBlock(index), row.getRawIndex());
        }

        @Override
        protected Object visitIdentifier(Identifier node, Object context)
        {
            // Identifier only exists before planning.
            // ExpressionInterpreter should only be invoked after planning.
            // As a result, this method should be unreachable.
            // However, RelationPlanner.visitUnnest and visitValues invokes evaluateConstantExpression.
            return ((SymbolResolver) context).getValue(new Symbol(node.getValue()));
        }

        @Override
        protected Object visitSymbolReference(SymbolReference node, Object context)
        {
            return ((SymbolResolver) context).getValue(Symbol.from(node));
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return literalInterpreter.evaluate(node, type(node));
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            return value == null;
        }

        @Override
        protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            return value != null;
        }

        @Override
        protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context)
        {
            Object newDefault = null;
            boolean foundNewDefault = false;

            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (Boolean.TRUE.equals(whenOperand)) {
                    // condition is true, use this as default
                    foundNewDefault = true;
                    newDefault = processWithExceptionHandling(whenClause.getResult(), context);
                    break;
                }
            }

            Object defaultResult;
            if (foundNewDefault) {
                defaultResult = newDefault;
            }
            else {
                defaultResult = processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
            }

            return defaultResult;
        }

        @Override
        protected Object visitIfExpression(IfExpression node, Object context)
        {
            Object condition = processWithExceptionHandling(node.getCondition(), context);

            if (Boolean.TRUE.equals(condition)) {
                return processWithExceptionHandling(node.getTrueValue(), context);
            }
            return processWithExceptionHandling(node.getFalseValue().orElse(null), context);
        }

        @Override
        protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Object context)
        {
            Object operand = processWithExceptionHandling(node.getOperand(), context);
            Type operandType = type(node.getOperand());

            // if operand is null, return defaultValue
            if (operand == null) {
                return processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
            }

            Object newDefault = null;
            boolean foundNewDefault = false;

            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (whenOperand != null && isEqual(operand, operandType, whenOperand, type(whenClause.getOperand()))) {
                    // condition is true, use this as default
                    foundNewDefault = true;
                    newDefault = processWithExceptionHandling(whenClause.getResult(), context);
                    break;
                }
            }

            Object defaultResult;
            if (foundNewDefault) {
                defaultResult = newDefault;
            }
            else {
                defaultResult = processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
            }

            return defaultResult;
        }

        private boolean isEqual(Object operand1, Type type1, Object operand2, Type type2)
        {
            return Boolean.TRUE.equals(invokeOperator(OperatorType.EQUAL, ImmutableList.of(type1, type2), ImmutableList.of(operand1, operand2)));
        }

        private Type type(Expression expression)
        {
            return expressionTypes.get(NodeRef.of(expression));
        }

        @Override
        protected Object visitCoalesceExpression(CoalesceExpression node, Object context)
        {
            for (Expression operand : node.getOperands()) {
                Object value = processWithExceptionHandling(operand, context);
                if (value != null) {
                    return value;
                }
            }

            return null;
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression valueList)) {
                throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
            }
            // `NULL IN ()` would be false, but InListExpression cannot be empty by construction
            if (value == null) {
                return null;
            }

            Set<?> set = inListCache.get(valueList);

            // We use the presence of the node in the map to indicate that we've already done
            // the analysis below. If the value is null, it means that we can't apply the HashSet
            // optimization
            if (!inListCache.containsKey(valueList)) {
                if (valueList.getValues().stream().allMatch(Literal.class::isInstance) &&
                        valueList.getValues().stream().noneMatch(NullLiteral.class::isInstance)) {
                    Set<Object> objectSet = valueList.getValues().stream().map(expression -> processWithExceptionHandling(expression, context)).collect(Collectors.toSet());
                    Type type = type(node.getValue());
                    set = FastutilSetHelper.toFastutilHashSet(
                            objectSet,
                            type,
                            plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(HASH_CODE, ImmutableList.of(type)), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle(),
                            plannerContext.getFunctionManager().getScalarFunctionImplementation(metadata.resolveOperator(EQUAL, ImmutableList.of(type, type)), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle());
                }
                inListCache.put(valueList, set);
            }

            if (set != null) {
                return set.contains(value);
            }

            boolean hasNullValue = false;
            boolean found = false;

            ResolvedFunction equalsOperator = metadata.resolveOperator(OperatorType.EQUAL, types(node.getValue(), valueList));
            for (Expression expression : valueList.getValues()) {
                // Use process() instead of processWithExceptionHandling() for processing in-list items.
                // Do not handle exceptions thrown while processing a single in-list expression,
                // but fail the whole in-predicate evaluation.
                // According to in-predicate semantics, all in-list items must be successfully evaluated
                // before a check for the match is performed.
                Object inValue = process(expression, context);
                if (inValue == null) {
                    hasNullValue = true;
                }
                else {
                    Boolean result = (Boolean) functionInvoker.invoke(equalsOperator, connectorSession, ImmutableList.of(value, inValue));
                    if (result == null) {
                        hasNullValue = true;
                    }
                    else if (!found && result) {
                        // in does not short-circuit so we must evaluate all value in the list
                        found = true;
                    }
                }
            }
            if (found) {
                return true;
            }

            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitExists(ExistsPredicate node, Object context)
        {
            throw new UnsupportedOperationException("Exists subquery not yet implemented");
        }

        @Override
        protected Object visitSubqueryExpression(SubqueryExpression node, Object context)
        {
            throw new UnsupportedOperationException("Subquery not yet implemented");
        }

        @Override
        protected Object visitArithmeticUnary(ArithmeticUnaryExpression node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            if (value == null) {
                return null;
            }

            return switch (node.getSign()) {
                case PLUS -> value;
                case MINUS -> {
                    ResolvedFunction resolvedOperator = metadata.resolveOperator(OperatorType.NEGATION, types(node.getValue()));
                    InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(NEVER_NULL), FAIL_ON_NULL, true, false);
                    MethodHandle handle = plannerContext.getFunctionManager().getScalarFunctionImplementation(resolvedOperator, invocationConvention).getMethodHandle();

                    if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == ConnectorSession.class) {
                        handle = handle.bindTo(connectorSession);
                    }
                    try {
                        yield handle.invokeWithArguments(value);
                    }
                    catch (Throwable throwable) {
                        throwIfInstanceOf(throwable, RuntimeException.class);
                        throwIfInstanceOf(throwable, Error.class);
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    }
                }
            };
        }

        @Override
        protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context)
        {
            Object left = processWithExceptionHandling(node.getLeft(), context);
            if (left == null) {
                return null;
            }
            Object right = processWithExceptionHandling(node.getRight(), context);
            if (right == null) {
                return null;
            }

            return invokeOperator(OperatorType.valueOf(node.getOperator().name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            Operator operator = node.getOperator();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            if (operator == Operator.IS_DISTINCT_FROM) {
                return processIsDistinctFrom(context, left, right);
            }
            // Execution engine does not have not equal and greater than operators, so interpret with
            // equal or less than, but do not flip operator in result, as many optimizers depend on
            // operators not flipping
            if (node.getOperator() == Operator.NOT_EQUAL) {
                Object result = visitComparisonExpression(flipComparison(node), context);
                if (result == null) {
                    return null;
                }
                return !(Boolean) result;
            }
            if (node.getOperator() == Operator.GREATER_THAN || node.getOperator() == Operator.GREATER_THAN_OR_EQUAL) {
                return visitComparisonExpression(flipComparison(node), context);
            }

            return processComparisonExpression(context, operator, left, right);
        }

        private Object processIsDistinctFrom(Object context, Expression leftExpression, Expression rightExpression)
        {
            Object left = processWithExceptionHandling(leftExpression, context);
            Object right = processWithExceptionHandling(rightExpression, context);

            return invokeOperator(OperatorType.valueOf(Operator.IS_DISTINCT_FROM.name()), types(leftExpression, rightExpression), Arrays.asList(left, right));
        }

        private Object processComparisonExpression(Object context, Operator operator, Expression leftExpression, Expression rightExpression)
        {
            Object left = processWithExceptionHandling(leftExpression, context);
            if (left == null) {
                return null;
            }

            Object right = processWithExceptionHandling(rightExpression, context);
            if (right == null) {
                return null;
            }

            return invokeOperator(OperatorType.valueOf(operator.name()), types(leftExpression, rightExpression), ImmutableList.of(left, right));
        }

        // TODO define method contract or split into separate methods, as flip(EQUAL) is a negation, while flip(LESS_THAN) is just flipping sides
        private ComparisonExpression flipComparison(ComparisonExpression comparisonExpression)
        {
            return switch (comparisonExpression.getOperator()) {
                case EQUAL -> new ComparisonExpression(Operator.NOT_EQUAL, comparisonExpression.getLeft(), comparisonExpression.getRight());
                case NOT_EQUAL -> new ComparisonExpression(Operator.EQUAL, comparisonExpression.getLeft(), comparisonExpression.getRight());
                case LESS_THAN -> new ComparisonExpression(Operator.GREATER_THAN, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case LESS_THAN_OR_EQUAL -> new ComparisonExpression(Operator.GREATER_THAN_OR_EQUAL, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case GREATER_THAN -> new ComparisonExpression(Operator.LESS_THAN, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case GREATER_THAN_OR_EQUAL -> new ComparisonExpression(Operator.LESS_THAN_OR_EQUAL, comparisonExpression.getRight(), comparisonExpression.getLeft());
                default -> throw new IllegalStateException("Unexpected value: " + comparisonExpression.getOperator());
            };
        }

        @Override
        protected Object visitBetweenPredicate(BetweenPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            if (value == null) {
                return null;
            }
            Object min = processWithExceptionHandling(node.getMin(), context);
            Object max = processWithExceptionHandling(node.getMax(), context);

            Boolean greaterOrEqualToMin = null;
            if (min != null) {
                greaterOrEqualToMin = (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL, types(node.getMin(), node.getValue()), ImmutableList.of(min, value));
            }
            Boolean lessThanOrEqualToMax = null;
            if (max != null) {
                lessThanOrEqualToMax = (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL, types(node.getValue(), node.getMax()), ImmutableList.of(value, max));
            }

            if (greaterOrEqualToMin == null) {
                return Objects.equals(lessThanOrEqualToMax, Boolean.FALSE) ? false : null;
            }
            if (lessThanOrEqualToMax == null) {
                return Objects.equals(greaterOrEqualToMin, Boolean.FALSE) ? false : null;
            }
            return greaterOrEqualToMin && lessThanOrEqualToMax;
        }

        @Override
        protected Object visitNullIfExpression(NullIfExpression node, Object context)
        {
            Object first = processWithExceptionHandling(node.getFirst(), context);
            if (first == null) {
                return null;
            }
            Object second = processWithExceptionHandling(node.getSecond(), context);
            if (second == null) {
                return first;
            }

            Type firstType = type(node.getFirst());
            Type secondType = type(node.getSecond());

            Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).get();

            ResolvedFunction firstCast = metadata.getCoercion(firstType, commonType);
            ResolvedFunction secondCast = metadata.getCoercion(secondType, commonType);

            // cast(first as <common type>) == cast(second as <common type>)
            boolean equal = Boolean.TRUE.equals(invokeOperator(
                    OperatorType.EQUAL,
                    ImmutableList.of(commonType, commonType),
                    ImmutableList.of(
                            functionInvoker.invoke(firstCast, connectorSession, ImmutableList.of(first)),
                            functionInvoker.invoke(secondCast, connectorSession, ImmutableList.of(second)))));

            if (equal) {
                return null;
            }
            return first;
        }

        @Override
        protected Object visitNotExpression(NotExpression node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            if (value == null) {
                return null;
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalExpression(LogicalExpression node, Object context)
        {
            boolean hasNull = false;
            for (Expression term : node.getTerms()) {
                Object processed = processWithExceptionHandling(term, context);

                if (processed == null) {
                    hasNull = true;
                }
                else {
                    switch (node.getOperator()) {
                        case AND -> {
                            if (Boolean.FALSE.equals(processed)) {
                                return false;
                            }
                        }
                        case OR -> {
                            if (Boolean.TRUE.equals(processed)) {
                                return true;
                            }
                        }
                    }
                }
            }

            if (hasNull) {
                return null;
            }

            return switch (node.getOperator()) {
                case AND -> true;
                case OR -> false;
            };
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Object context)
        {
            return node.equals(BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        protected Object visitFunctionCall(FunctionCall node, Object context)
        {
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.getArguments()) {
                Object value = processWithExceptionHandling(expression, context);
                argumentValues.add(value);
            }

            ResolvedFunction resolvedFunction = resolvedFunctions.get(NodeRef.of(node));
            FunctionNullability functionNullability = resolvedFunction.getFunctionNullability();
            for (int i = 0; i < argumentValues.size(); i++) {
                Object value = argumentValues.get(i);
                if (value == null && !functionNullability.isArgumentNullable(i)) {
                    return null;
                }
            }

            // do not optimize non-deterministic functions
            return functionInvoker.invoke(resolvedFunction, connectorSession, argumentValues);
        }

        @Override
        protected Object visitLambdaExpression(LambdaExpression node, Object context)
        {
            Expression body = node.getBody();
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());
            FunctionType functionType = (FunctionType) expressionTypes.get(NodeRef.<Expression>of(node));
            checkArgument(argumentNames.size() == functionType.getArgumentTypes().size());

            return generateVarArgsToMapAdapter(
                    Primitives.wrap(functionType.getReturnType().getJavaType()),
                    functionType.getArgumentTypes().stream()
                            .map(Type::getJavaType)
                            .map(Primitives::wrap)
                            .collect(toImmutableList()),
                    argumentNames,
                    map -> processWithExceptionHandling(body, new LambdaSymbolResolver(map)));
        }

        @Override
        protected Object visitBindExpression(BindExpression node, Object context)
        {
            Object[] values = node.getValues().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .toArray(); // values are nullable
            Object function = processWithExceptionHandling(node.getFunction(), context);

            return MethodHandles.insertArguments((MethodHandle) function, 0, values);
        }

        @Override
        protected Object visitLikePredicate(LikePredicate node, Object context)
        {
            Slice value = (Slice) processWithExceptionHandling(node.getValue(), context);

            if (value == null) {
                return null;
            }

            if (node.getPattern() instanceof StringLiteral && (node.getEscape().isEmpty() || node.getEscape().get() instanceof StringLiteral)) {
                // fast path when we know the pattern and escape are constant
                LikePattern pattern = getConstantPattern(node);
                if (type(node.getValue()) instanceof VarcharType) {
                    return LikeFunctions.likeVarchar(value, pattern);
                }

                Type type = type(node.getValue());
                checkState(type instanceof CharType, "LIKE value is neither VARCHAR or CHAR");
                return LikeFunctions.likeChar((long) ((CharType) type).getLength(), value, pattern);
            }

            Slice pattern = (Slice) processWithExceptionHandling(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Slice escape = null;
            if (node.getEscape().isPresent()) {
                escape = (Slice) processWithExceptionHandling(node.getEscape().get(), context);

                if (escape == null) {
                    return null;
                }
            }

            LikePattern likePattern;
            if (escape == null) {
                likePattern = LikePattern.compile(pattern.toStringUtf8(), Optional.empty());
            }
            else {
                likePattern = LikeFunctions.likePattern(pattern, escape);
            }

            if (type(node.getValue()) instanceof VarcharType) {
                return LikeFunctions.likeVarchar(value, likePattern);
            }

            Type type = type(node.getValue());
            checkState(type instanceof CharType, "LIKE value is neither VARCHAR or CHAR");
            return LikeFunctions.likeChar((long) ((CharType) type).getLength(), value, likePattern);
        }

        private LikePattern getConstantPattern(LikePredicate node)
        {
            LikePattern result = likePatternCache.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();

                if (node.getEscape().isPresent()) {
                    Slice escape = Slices.utf8Slice(((StringLiteral) node.getEscape().get()).getValue());
                    result = LikeFunctions.likePattern(Slices.utf8Slice(pattern.getValue()), escape);
                }
                else {
                    result = LikePattern.compile(pattern.getValue(), Optional.empty());
                }

                likePatternCache.put(node, result);
            }

            return result;
        }

        @Override
        public Object visitCast(Cast node, Object context)
        {
            Object value = processWithExceptionHandling(node.getExpression(), context);
            Type targetType = plannerContext.getTypeManager().getType(toTypeSignature(node.getType()));
            Type sourceType = type(node.getExpression());

            if (value == null) {
                return null;
            }

            ResolvedFunction operator = metadata.getCoercion(sourceType, targetType);

            try {
                return functionInvoker.invoke(operator, connectorSession, ImmutableList.of(value));
            }
            catch (RuntimeException e) {
                if (node.isSafe()) {
                    return null;
                }
                throw e;
            }
        }

        @Override
        protected Object visitArray(Array node, Object context)
        {
            Type elementType = ((ArrayType) type(node)).getElementType();
            BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, node.getValues().size());

            for (Expression expression : node.getValues()) {
                Object value = processWithExceptionHandling(expression, context);
                writeNativeValue(elementType, arrayBlockBuilder, value);
            }

            return arrayBlockBuilder.build();
        }

        @Override
        protected Object visitCurrentCatalog(CurrentCatalog node, Object context)
        {
            FunctionCall function = BuiltinFunctionCallBuilder.resolve(metadata)
                    .setName("$current_catalog")
                    .build();

            return visitFunctionCall(function, context);
        }

        @Override
        protected Object visitCurrentSchema(CurrentSchema node, Object context)
        {
            FunctionCall function = BuiltinFunctionCallBuilder.resolve(metadata)
                    .setName("$current_schema")
                    .build();

            return visitFunctionCall(function, context);
        }

        @Override
        protected Object visitCurrentUser(CurrentUser node, Object context)
        {
            FunctionCall function = BuiltinFunctionCallBuilder.resolve(metadata)
                    .setName("$current_user")
                    .build();

            return visitFunctionCall(function, context);
        }

        @Override
        protected Object visitCurrentPath(CurrentPath node, Object context)
        {
            FunctionCall function = BuiltinFunctionCallBuilder.resolve(metadata)
                    .setName("$current_path")
                    .build();

            return visitFunctionCall(function, context);
        }

        @Override
        protected Object visitAtTimeZone(AtTimeZone node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            if (value == null) {
                return null;
            }

            Object timeZone = processWithExceptionHandling(node.getTimeZone(), context);
            if (timeZone == null) {
                return null;
            }

            Type valueType = type(node.getValue());
            Type timeZoneType = type(node.getTimeZone());

            if (valueType instanceof TimeType type) {
                // <time> AT TIME ZONE <tz> gets desugared as $at_timezone(cast(<time> AS TIME(p) WITH TIME ZONE, <tz>)
                TimeWithTimeZoneType timeWithTimeZoneType = createTimeWithTimeZoneType(type.getPrecision());

                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("$at_timezone", TypeSignatureProvider.fromTypes(timeWithTimeZoneType, timeZoneType));

                ResolvedFunction cast = metadata.getCoercion(valueType, timeWithTimeZoneType);
                return functionInvoker.invoke(function, connectorSession, ImmutableList.of(
                        functionInvoker.invoke(cast, connectorSession, ImmutableList.of(value)),
                        timeZone));
            }

            if (valueType instanceof TimeWithTimeZoneType) {
                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("$at_timezone", TypeSignatureProvider.fromTypes(valueType, timeZoneType));

                return functionInvoker.invoke(function, connectorSession, ImmutableList.of(value, timeZone));
            }

            if (valueType instanceof TimestampType type) {
                // <timestamp> AT TIME ZONE <tz> gets desugared as at_timezone(cast(<timestamp> AS TIMESTAMP(p) WITH TIME ZONE, <tz>)
                TimestampWithTimeZoneType timestampWithTimeZoneType = createTimestampWithTimeZoneType(type.getPrecision());

                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("at_timezone", TypeSignatureProvider.fromTypes(timestampWithTimeZoneType, timeZoneType));

                ResolvedFunction cast = metadata.getCoercion(valueType, timestampWithTimeZoneType);
                return functionInvoker.invoke(function, connectorSession, ImmutableList.of(
                        functionInvoker.invoke(cast, connectorSession, ImmutableList.of(value)),
                        timeZone));
            }

            if (valueType instanceof TimestampWithTimeZoneType) {
                ResolvedFunction function = plannerContext.getMetadata()
                        .resolveBuiltinFunction("at_timezone", TypeSignatureProvider.fromTypes(valueType, timeZoneType));

                return functionInvoker.invoke(function, connectorSession, ImmutableList.of(value, timeZone));
            }

            throw new IllegalArgumentException("Unexpected type: " + valueType);
        }

        @Override
        protected Object visitCurrentDate(CurrentDate node, Object context)
        {
            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction("current_date", ImmutableList.of()),
                    connectorSession,
                    ImmutableList.of());
        }

        @Override
        protected Object visitCurrentTime(CurrentTime node, Object context)
        {
            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction("$current_time", TypeSignatureProvider.fromTypes(type(node))),
                    connectorSession,
                    singletonList(null));
        }

        @Override
        protected Object visitCurrentTimestamp(CurrentTimestamp node, Object context)
        {
            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction("$current_timestamp", TypeSignatureProvider.fromTypes(type(node))),
                    connectorSession,
                    singletonList(null));
        }

        @Override
        protected Object visitLocalTime(LocalTime node, Object context)
        {
            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction("$localtime", TypeSignatureProvider.fromTypes(type(node))),
                    connectorSession,
                    singletonList(null));
        }

        @Override
        protected Object visitLocalTimestamp(LocalTimestamp node, Object context)
        {
            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction("$localtimestamp", TypeSignatureProvider.fromTypes(type(node))),
                    connectorSession,
                    singletonList(null));
        }

        @Override
        protected Object visitRow(Row node, Object context)
        {
            RowType rowType = (RowType) type(node);
            List<Type> parameterTypes = rowType.getTypeParameters();
            List<Expression> arguments = node.getItems();

            int cardinality = arguments.size();
            return buildRowValue(rowType, fields -> {
                for (int i = 0; i < cardinality; ++i) {
                    writeNativeValue(parameterTypes.get(i), fields.get(i), processWithExceptionHandling(arguments.get(i), context));
                }
            });
        }

        @Override
        protected Object visitFormat(Format node, Object context)
        {
            Object format = processWithExceptionHandling(node.getArguments().get(0), context);
            if (format == null) {
                return null;
            }

            // FORMAT(a, b, c, d, ...) gets desugared into $format(a, row(b, c, d, ...))
            List<Expression> arguments = node.getArguments().subList(1, node.getArguments().size());
            List<Type> argumentTypes = arguments.stream()
                    .map(this::type)
                    .collect(toImmutableList());

            RowType rowType = anonymous(argumentTypes);
            ResolvedFunction function = plannerContext.getMetadata()
                    .resolveBuiltinFunction(FormatFunction.NAME, TypeSignatureProvider.fromTypes(VARCHAR, rowType));

            // Construct a row with arguments [1..n] and invoke the underlying function
            SqlRow row = buildRowValue(rowType, fields -> {
                for (int i = 0; i < arguments.size(); ++i) {
                    writeNativeValue(argumentTypes.get(i), fields.get(i), processWithExceptionHandling(arguments.get(i), context));
                }
            });
            return functionInvoker.invoke(
                    function,
                    connectorSession,
                    ImmutableList.of(format, row));
        }

        @Override
        protected Object visitSubscriptExpression(SubscriptExpression node, Object context)
        {
            Object base = processWithExceptionHandling(node.getBase(), context);
            if (base == null) {
                return null;
            }
            Object index = processWithExceptionHandling(node.getIndex(), context);
            if (index == null) {
                return null;
            }
            if ((index instanceof Long) && isArray(type(node.getBase()))) {
                ArraySubscriptOperator.checkArrayIndex((Long) index);
            }

            // Subscript on Row hasn't got a dedicated operator. It is interpreted by hand.
            if (base instanceof SqlRow row) {
                int fieldIndex = toIntExact((long) index - 1);
                if (fieldIndex < 0 || fieldIndex >= row.getFieldCount()) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ROW index out of bounds: " + (fieldIndex + 1));
                }
                Type returnType = type(node.getBase()).getTypeParameters().get(fieldIndex);
                return readNativeValue(returnType, row.getRawFieldBlock(fieldIndex), row.getRawIndex());
            }

            // Subscript on Array or Map is interpreted using operator.
            return invokeOperator(OperatorType.SUBSCRIPT, types(node.getBase(), node.getIndex()), ImmutableList.of(base, index));
        }

        @Override
        protected Object visitExtract(Extract node, Object context)
        {
            Object value = processWithExceptionHandling(node.getExpression(), context);
            if (value == null) {
                return null;
            }

            String name = switch (node.getField()) {
                case YEAR -> "year";
                case QUARTER -> "quarter";
                case MONTH -> "month";
                case WEEK -> "week";
                case DAY, DAY_OF_MONTH -> "day";
                case DAY_OF_WEEK, DOW -> "day_of_week";
                case DAY_OF_YEAR, DOY -> "day_of_year";
                case YEAR_OF_WEEK, YOW -> "year_of_week";
                case HOUR -> "hour";
                case MINUTE -> "minute";
                case SECOND -> "second";
                case TIMEZONE_MINUTE -> "timezone_minute";
                case TIMEZONE_HOUR -> "timezone_hour";
            };

            return functionInvoker.invoke(
                    plannerContext.getMetadata()
                            .resolveBuiltinFunction(name, TypeSignatureProvider.fromTypes(type(node.getExpression()))),
                    connectorSession,
                    ImmutableList.of(value));
        }

        @Override
        protected Object visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Object context)
        {
            throw new UnsupportedOperationException("QuantifiedComparison not yet implemented");
        }

        @Override
        protected Object visitExpression(Expression node, Object context)
        {
            throw new TrinoException(NOT_SUPPORTED, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Object visitNode(Node node, Object context)
        {
            throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
        }

        private List<Type> types(Expression... expressions)
        {
            return Stream.of(expressions)
                    .map(NodeRef::of)
                    .map(expressionTypes::get)
                    .collect(toImmutableList());
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            ResolvedFunction operator = metadata.resolveOperator(operatorType, argumentTypes);
            return functionInvoker.invoke(operator, connectorSession, argumentValues);
        }
    }

    private static boolean isArray(Type type)
    {
        return type instanceof ArrayType;
    }

    private static class LambdaSymbolResolver
            implements SymbolResolver
    {
        private final Map<String, Object> values;

        public LambdaSymbolResolver(Map<String, Object> values)
        {
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public Object getValue(Symbol symbol)
        {
            checkState(values.containsKey(symbol.getName()), "values does not contain %s", symbol);
            return values.get(symbol.getName());
        }
    }
}
