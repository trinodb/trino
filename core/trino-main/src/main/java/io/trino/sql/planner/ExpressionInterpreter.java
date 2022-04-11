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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.ArraySubscriptOperator;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.iterative.rule.DesugarCurrentPath;
import io.trino.sql.planner.iterative.rule.DesugarCurrentUser;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.ComparisonExpression.Operator;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
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
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
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
import io.trino.type.JoniRegexp;
import io.trino.type.LikeFunctions;
import io.trino.type.TypeCoercion;
import io.trino.util.FastutilSetHelper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.gen.VarArgsToMapAdapterGenerator.generateVarArgsToMapAdapter;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ResolvedFunctionCallRewriter.rewriteResolvedFunctions;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.DesugarCurrentCatalog.desugarCurrentCatalog;
import static io.trino.sql.planner.iterative.rule.DesugarCurrentSchema.desugarCurrentSchema;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.DereferenceExpression.isQualifiedAllFieldsReference;
import static io.trino.type.LikeFunctions.isLikePattern;
import static io.trino.type.LikeFunctions.unescapeLiteralLikePattern;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ExpressionInterpreter
{
    private final Expression expression;
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;
    private final Session session;
    private final ConnectorSession connectorSession;
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final InterpretedFunctionInvoker functionInvoker;
    private final TypeCoercion typeCoercion;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, JoniRegexp> likePatternCache = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();

    public ExpressionInterpreter(Expression expression, PlannerContext plannerContext, Session session, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.literalEncoder = new LiteralEncoder(plannerContext);
        this.session = requireNonNull(session, "session is null");
        this.connectorSession = session.toConnectorSession();
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        verify((expressionTypes.containsKey(NodeRef.of(expression))));
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
        ExpressionAnalyzer analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(expression, Scope.create());

        Type actualType = analyzer.getExpressionTypes().get(NodeRef.of(expression));
        if (!new TypeCoercion(plannerContext.getTypeManager()::getType).canCoerce(actualType, expectedType)) {
            throw semanticException(TYPE_MISMATCH, expression, "Cannot cast type %s to %s", actualType.getDisplayName(), expectedType.getDisplayName());
        }

        Map<NodeRef<Expression>, Type> coercions = ImmutableMap.<NodeRef<Expression>, Type>builder()
                .putAll(analyzer.getExpressionCoercions())
                .put(NodeRef.of(expression), expectedType)
                .buildOrThrow();
        return evaluateConstantExpression(expression, coercions, analyzer.getTypeOnlyCoercions(), plannerContext, session, accessControl, ImmutableSet.of(), parameters);
    }

    public static Object evaluateConstantExpression(
            Expression expression,
            Map<NodeRef<Expression>, Type> coercions,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            PlannerContext plannerContext,
            Session session,
            AccessControl accessControl,
            Set<NodeRef<Expression>> columnReferences,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        requireNonNull(columnReferences, "columnReferences is null");

        verifyExpressionIsConstant(columnReferences, expression);

        // add coercions
        Expression rewrite = Coercer.addCoercions(expression, coercions, typeOnlyCoercions);

        // redo the analysis since above expression rewriter might create new expressions which do not have entries in the type map
        ExpressionAnalyzer analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(rewrite, Scope.create());

        // remove syntax sugar
        rewrite = DesugarAtTimeZoneRewriter.rewrite(rewrite, analyzer.getExpressionTypes(), plannerContext.getMetadata(), session);

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

        // resolve functions
        Expression resolved = rewriteResolvedFunctions(canonicalized, analyzer.getResolvedFunctions());

        // The optimization above may have rewritten the expression tree which breaks all the identity maps, so redo the analysis
        // to re-analyze coercions that might be necessary
        analyzer = createConstantAnalyzer(plannerContext, accessControl, session, parameters, WarningCollector.NOOP);
        analyzer.analyze(resolved, Scope.create());

        // evaluate the expression
        return new ExpressionInterpreter(resolved, plannerContext, session, analyzer.getExpressionTypes()).evaluate();
    }

    public Type getType()
    {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Object evaluate()
    {
        Object result = new Visitor(false).processWithExceptionHandling(expression, new NoPagePositionContext());
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    public Object evaluate(SymbolResolver inputs)
    {
        Object result = new Visitor(false).processWithExceptionHandling(expression, inputs);
        verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
        return result;
    }

    public Object optimize(SymbolResolver inputs)
    {
        return new Visitor(true).processWithExceptionHandling(expression, inputs);
    }

    private class Visitor
            extends AstVisitor<Object, Object>
    {
        private final boolean optimize;

        private Visitor(boolean optimize)
        {
            this.optimize = optimize;
        }

        private Object processWithExceptionHandling(Expression expression, Object context)
        {
            if (expression == null) {
                return null;
            }

            try {
                return process(expression, context);
            }
            catch (TrinoException e) {
                if (optimize) {
                    // Certain operations like 0 / 0 or likeExpression may throw exceptions.
                    // When optimizing, do not throw the exception, but delay it until the expression is actually executed.
                    // This is to take advantage of the possibility that some other optimization removes the erroneous
                    // expression from the plan.
                    return expression;
                }
                // Do not suppress exceptions during expression execution.
                throw e;
            }
        }

        @Override
        public Object visitFieldReference(FieldReference node, Object context)
        {
            throw new UnsupportedOperationException("Field references not supported in interpreter");
        }

        @Override
        protected Object visitDereferenceExpression(DereferenceExpression node, Object context)
        {
            checkArgument(!isQualifiedAllFieldsReference(node), "unexpected expression: all fields labeled reference " + node);
            Identifier fieldIdentifier = node.getField().orElseThrow();

            Type type = type(node.getBase());
            // if there is no type for the base of Dereference, it must be QualifiedName
            if (type == null) {
                return node;
            }

            // Row dereference: process dereference base eagerly, and only then pick the expected field
            Object base = processWithExceptionHandling(node.getBase(), context);
            // if the base part is evaluated to be null, the dereference expression should also be null
            if (base == null) {
                return null;
            }

            if (hasUnresolvedValue(base)) {
                return new DereferenceExpression(toExpression(base, type), fieldIdentifier);
            }

            RowType rowType = (RowType) type;
            Block row = (Block) base;
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
            return readNativeValue(returnType, row, index);
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
        protected Object visitParameter(Parameter node, Object context)
        {
            return node;
        }

        @Override
        protected Object visitSymbolReference(SymbolReference node, Object context)
        {
            return ((SymbolResolver) context).getValue(Symbol.from(node));
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return LiteralInterpreter.evaluate(plannerContext, session, expressionTypes, node);
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNullPredicate(toExpression(value, type(node.getValue())));
            }

            return value == null;
        }

        @Override
        protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNotNullPredicate(toExpression(value, type(node.getValue())));
            }

            return value != null;
        }

        @Override
        protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context)
        {
            Object newDefault = null;
            boolean foundNewDefault = false;

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (whenOperand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, type(whenClause.getOperand())),
                            toExpression(processWithExceptionHandling(whenClause.getResult(), context), type(whenClause.getResult()))));
                }
                else if (Boolean.TRUE.equals(whenOperand)) {
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

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            Expression defaultExpression = (defaultResult == null) ? null : toExpression(defaultResult, type(node));
            return new SearchedCaseExpression(whenClauses, Optional.ofNullable(defaultExpression));
        }

        @Override
        protected Object visitIfExpression(IfExpression node, Object context)
        {
            Object condition = processWithExceptionHandling(node.getCondition(), context);

            if (condition instanceof Expression) {
                Object trueValue = processWithExceptionHandling(node.getTrueValue(), context);
                Object falseValue = processWithExceptionHandling(node.getFalseValue().orElse(null), context);
                return new IfExpression(
                        toExpression(condition, type(node.getCondition())),
                        toExpression(trueValue, type(node.getTrueValue())),
                        (falseValue == null) ? null : toExpression(falseValue, type(node.getFalseValue().get())));
            }
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

            List<WhenClause> whenClauses = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

                if (whenOperand instanceof Expression || operand instanceof Expression) {
                    // cannot fully evaluate, add updated whenClause
                    whenClauses.add(new WhenClause(
                            toExpression(whenOperand, type(whenClause.getOperand())),
                            toExpression(processWithExceptionHandling(whenClause.getResult(), context), type(whenClause.getResult()))));
                }
                else if (whenOperand != null && isEqual(operand, operandType, whenOperand, type(whenClause.getOperand()))) {
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

            if (whenClauses.isEmpty()) {
                return defaultResult;
            }

            Expression defaultExpression = (defaultResult == null) ? null : toExpression(defaultResult, type(node));
            return new SimpleCaseExpression(toExpression(operand, type(node.getOperand())), whenClauses, Optional.ofNullable(defaultExpression));
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
            List<Object> newOperands = processOperands(node, context);
            if (newOperands.isEmpty()) {
                return null;
            }
            if (newOperands.size() == 1) {
                return getOnlyElement(newOperands);
            }
            return new CoalesceExpression(newOperands.stream()
                    .map(value -> toExpression(value, type(node)))
                    .collect(toImmutableList()));
        }

        private List<Object> processOperands(CoalesceExpression node, Object context)
        {
            List<Object> newOperands = new ArrayList<>();
            Set<Expression> uniqueNewOperands = new HashSet<>();
            for (Expression operand : node.getOperands()) {
                Object value = processWithExceptionHandling(operand, context);
                if (value instanceof CoalesceExpression) {
                    // The nested CoalesceExpression was recursively processed. It does not contain null.
                    for (Expression nestedOperand : ((CoalesceExpression) value).getOperands()) {
                        // Skip duplicates unless they are non-deterministic.
                        if (!isDeterministic(nestedOperand, metadata) || uniqueNewOperands.add(nestedOperand)) {
                            newOperands.add(nestedOperand);
                        }
                        // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
                        if (isEffectivelyLiteral(plannerContext, session, nestedOperand)) {
                            verify(
                                    !(nestedOperand instanceof NullLiteral) && !(nestedOperand instanceof Cast && ((Cast) nestedOperand).getExpression() instanceof NullLiteral),
                                    "Null operand should have been removed by recursive coalesce processing");
                            return newOperands;
                        }
                    }
                }
                else if (value instanceof Expression) {
                    verify(!(value instanceof NullLiteral), "Null value is expected to be represented as null, not NullLiteral");
                    // Skip duplicates unless they are non-deterministic.
                    Expression expression = (Expression) value;
                    if (!isDeterministic(expression, metadata) || uniqueNewOperands.add(expression)) {
                        newOperands.add(expression);
                    }
                }
                else if (value != null) {
                    // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
                    newOperands.add(value);
                    return newOperands;
                }
            }
            return newOperands;
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                if (!optimize) {
                    throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
                }
                return node;
            }
            InListExpression valueList = (InListExpression) valueListExpression;
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
                            plannerContext.getFunctionManager().getScalarFunctionInvoker(metadata.resolveOperator(session, HASH_CODE, ImmutableList.of(type)), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle(),
                            plannerContext.getFunctionManager().getScalarFunctionInvoker(metadata.resolveOperator(session, EQUAL, ImmutableList.of(type, type)), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle());
                }
                inListCache.put(valueList, set);
            }

            if (set != null && !(value instanceof Expression)) {
                return set.contains(value);
            }

            boolean hasUnresolvedValue = value instanceof Expression;
            boolean hasNullValue = false;
            boolean found = false;
            List<Object> values = new ArrayList<>(valueList.getValues().size());
            List<Type> types = new ArrayList<>(valueList.getValues().size());

            ResolvedFunction equalsOperator = metadata.resolveOperator(session, OperatorType.EQUAL, types(node.getValue(), valueList));
            for (Expression expression : valueList.getValues()) {
                // Use process() instead of processWithExceptionHandling() for processing in-list items.
                // Do not handle exceptions thrown while processing a single in-list expression,
                // but fail the whole in-predicate evaluation.
                // According to in-predicate semantics, all in-list items must be successfully evaluated
                // before a check for the match is performed.
                Object inValue = process(expression, context);
                if (value instanceof Expression || inValue instanceof Expression) {
                    hasUnresolvedValue = true;
                    values.add(inValue);
                    types.add(type(expression));
                    continue;
                }

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

            if (hasUnresolvedValue) {
                Type type = type(node.getValue());
                List<Expression> expressionValues = toExpressions(values, types);
                List<Expression> simplifiedExpressionValues = Stream.concat(
                                expressionValues.stream()
                                        .filter(expression -> isDeterministic(expression, metadata))
                                        .distinct(),
                                expressionValues.stream()
                                        .filter((expression -> !isDeterministic(expression, metadata))))
                        .collect(toImmutableList());

                if (simplifiedExpressionValues.size() == 1) {
                    return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, toExpression(value, type), simplifiedExpressionValues.get(0));
                }

                return new InPredicate(toExpression(value, type), new InListExpression(simplifiedExpressionValues));
            }
            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitExists(ExistsPredicate node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("Exists subquery not yet implemented");
            }
            return node;
        }

        @Override
        protected Object visitSubqueryExpression(SubqueryExpression node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("Subquery not yet implemented");
            }
            return node;
        }

        @Override
        protected Object visitArithmeticUnary(ArithmeticUnaryExpression node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);
            if (value == null) {
                return null;
            }
            if (value instanceof Expression) {
                Expression valueExpression = toExpression(value, type(node.getValue()));
                switch (node.getSign()) {
                    case PLUS:
                        return valueExpression;
                    case MINUS:
                        if (valueExpression instanceof ArithmeticUnaryExpression && ((ArithmeticUnaryExpression) valueExpression).getSign().equals(MINUS)) {
                            return ((ArithmeticUnaryExpression) valueExpression).getValue();
                        }
                        return new ArithmeticUnaryExpression(MINUS, valueExpression);
                    default:
                        throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
                }
            }

            switch (node.getSign()) {
                case PLUS:
                    return value;
                case MINUS:
                    ResolvedFunction resolvedOperator = metadata.resolveOperator(session, OperatorType.NEGATION, types(node.getValue()));
                    InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(NEVER_NULL), FAIL_ON_NULL, true, false);
                    MethodHandle handle = plannerContext.getFunctionManager().getScalarFunctionInvoker(resolvedOperator, invocationConvention).getMethodHandle();

                    if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == ConnectorSession.class) {
                        handle = handle.bindTo(connectorSession);
                    }
                    try {
                        return handle.invokeWithArguments(value);
                    }
                    catch (Throwable throwable) {
                        throwIfInstanceOf(throwable, RuntimeException.class);
                        throwIfInstanceOf(throwable, Error.class);
                        throw new RuntimeException(throwable.getMessage(), throwable);
                    }
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
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

            if (hasUnresolvedValue(left, right)) {
                return new ArithmeticBinaryExpression(node.getOperator(), toExpression(left, type(node.getLeft())), toExpression(right, type(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(node.getOperator().name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            ComparisonExpression.Operator operator = node.getOperator();
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
                if (result instanceof ComparisonExpression) {
                    return flipComparison((ComparisonExpression) result);
                }
                return !(Boolean) result;
            }
            if (node.getOperator() == Operator.GREATER_THAN || node.getOperator() == Operator.GREATER_THAN_OR_EQUAL) {
                Object result = visitComparisonExpression(flipComparison(node), context);
                if (result instanceof ComparisonExpression) {
                    return flipComparison((ComparisonExpression) result);
                }
                return result;
            }

            return processComparisonExpression(context, operator, left, right);
        }

        private Object processIsDistinctFrom(Object context, Expression leftExpression, Expression rightExpression)
        {
            Object left = processWithExceptionHandling(leftExpression, context);
            Object right = processWithExceptionHandling(rightExpression, context);

            if (left == null && right instanceof Expression) {
                return new IsNotNullPredicate((Expression) right);
            }

            if (right == null && left instanceof Expression) {
                return new IsNotNullPredicate((Expression) left);
            }

            if (left instanceof Expression || right instanceof Expression) {
                return new ComparisonExpression(Operator.IS_DISTINCT_FROM, toExpression(left, type(leftExpression)), toExpression(right, type(rightExpression)));
            }

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

            if (left instanceof Expression || right instanceof Expression) {
                return new ComparisonExpression(operator, toExpression(left, type(leftExpression)), toExpression(right, type(rightExpression)));
            }

            return invokeOperator(OperatorType.valueOf(operator.name()), types(leftExpression, rightExpression), ImmutableList.of(left, right));
        }

        // TODO define method contract or split into separate methods, as flip(EQUAL) is a negation, while flip(LESS_THAN) is just flipping sides
        private ComparisonExpression flipComparison(ComparisonExpression comparisonExpression)
        {
            switch (comparisonExpression.getOperator()) {
                case EQUAL:
                    return new ComparisonExpression(Operator.NOT_EQUAL, comparisonExpression.getLeft(), comparisonExpression.getRight());
                case NOT_EQUAL:
                    return new ComparisonExpression(Operator.EQUAL, comparisonExpression.getLeft(), comparisonExpression.getRight());
                case LESS_THAN:
                    return new ComparisonExpression(Operator.GREATER_THAN, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case LESS_THAN_OR_EQUAL:
                    return new ComparisonExpression(Operator.GREATER_THAN_OR_EQUAL, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case GREATER_THAN:
                    return new ComparisonExpression(Operator.LESS_THAN, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case GREATER_THAN_OR_EQUAL:
                    return new ComparisonExpression(Operator.LESS_THAN_OR_EQUAL, comparisonExpression.getRight(), comparisonExpression.getLeft());
                case IS_DISTINCT_FROM:
                    // not expected here
            }
            throw new IllegalArgumentException("Unsupported comparison type: " + comparisonExpression.getOperator());
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

            if (value instanceof Expression || min instanceof Expression || max instanceof Expression) {
                return new BetweenPredicate(
                        toExpression(value, type(node.getValue())),
                        toExpression(min, type(node.getMin())),
                        toExpression(max, type(node.getMax())));
            }

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

            if (hasUnresolvedValue(first, second)) {
                return new NullIfExpression(toExpression(first, firstType), toExpression(second, secondType));
            }

            Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).get();

            ResolvedFunction firstCast = metadata.getCoercion(session, firstType, commonType);
            ResolvedFunction secondCast = metadata.getCoercion(session, secondType, commonType);

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

            if (value instanceof Expression) {
                return new NotExpression(toExpression(value, type(node.getValue())));
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalExpression(LogicalExpression node, Object context)
        {
            List<Object> terms = new ArrayList<>();
            List<Type> types = new ArrayList<>();

            for (Expression term : node.getTerms()) {
                Object processed = processWithExceptionHandling(term, context);

                switch (node.getOperator()) {
                    case AND:
                        if (Boolean.FALSE.equals(processed)) {
                            return false;
                        }

                        if (!Boolean.TRUE.equals(processed)) {
                            terms.add(processed);
                            types.add(type(term));
                        }

                        break;
                    case OR:
                        if (Boolean.TRUE.equals(processed)) {
                            return true;
                        }

                        if (!Boolean.FALSE.equals(processed)) {
                            terms.add(processed);
                            types.add(type(term));
                        }
                        break;
                }
            }

            if (terms.isEmpty()) {
                switch (node.getOperator()) {
                    case AND:
                        // terms are true
                        return true;
                    case OR:
                        // all terms are false
                        return false;
                }
            }

            if (terms.size() == 1) {
                return terms.get(0);
            }

            if (terms.stream().allMatch(Objects::isNull)) {
                return null;
            }

            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            for (int i = 0; i < terms.size(); i++) {
                expressions.add(toExpression(terms.get(i), types.get(i)));
            }
            return new LogicalExpression(node.getOperator(), expressions.build());
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Object context)
        {
            return node.equals(BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        protected Object visitFunctionCall(FunctionCall node, Object context)
        {
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.getArguments()) {
                Object value = processWithExceptionHandling(expression, context);
                Type type = type(expression);
                argumentValues.add(value);
                argumentTypes.add(type);
            }

            ResolvedFunction resolvedFunction = metadata.decodeFunction(node.getName());
            FunctionNullability functionNullability = resolvedFunction.getFunctionNullability();
            for (int i = 0; i < argumentValues.size(); i++) {
                Object value = argumentValues.get(i);
                if (value == null && !functionNullability.isArgumentNullable(i)) {
                    return null;
                }
            }

            // do not optimize non-deterministic functions
            if (optimize && (!metadata.getFunctionMetadata(resolvedFunction).isDeterministic() ||
                    hasUnresolvedValue(argumentValues) ||
                    isDynamicFilter(node) ||
                    resolvedFunction.getSignature().getName().equals("fail"))) {
                verify(!node.isDistinct(), "distinct not supported");
                verify(node.getOrderBy().isEmpty(), "order by not supported");
                verify(node.getFilter().isEmpty(), "filter not supported");
                return FunctionCallBuilder.resolve(session, metadata)
                        .setName(node.getName())
                        .setWindow(node.getWindow())
                        .setArguments(argumentTypes, toExpressions(argumentValues, argumentTypes))
                        .build();
            }
            return functionInvoker.invoke(resolvedFunction, connectorSession, argumentValues);
        }

        @Override
        protected Object visitLambdaExpression(LambdaExpression node, Object context)
        {
            if (optimize) {
                // TODO: enable optimization related to lambda expression
                // A mechanism to convert function type back into lambda expression need to exist to enable optimization
                return node;
            }

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
            List<Object> values = node.getValues().stream()
                    .map(value -> processWithExceptionHandling(value, context))
                    .collect(toList()); // values are nullable
            Object function = processWithExceptionHandling(node.getFunction(), context);

            if (hasUnresolvedValue(values) || hasUnresolvedValue(function)) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                for (int i = 0; i < values.size(); i++) {
                    builder.add(toExpression(values.get(i), type(node.getValues().get(i))));
                }

                return new BindExpression(
                        builder.build(),
                        toExpression(function, type(node.getFunction())));
            }

            return MethodHandles.insertArguments((MethodHandle) function, 0, values.toArray());
        }

        @Override
        protected Object visitLikePredicate(LikePredicate node, Object context)
        {
            Object value = processWithExceptionHandling(node.getValue(), context);

            if (value == null) {
                return null;
            }

            if (value instanceof Slice &&
                    node.getPattern() instanceof StringLiteral &&
                    (node.getEscape().isEmpty() || node.getEscape().get() instanceof StringLiteral)) {
                // fast path when we know the pattern and escape are constant
                return evaluateLikePredicate(node, (Slice) value, getConstantPattern(node));
            }

            Object pattern = processWithExceptionHandling(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Object escape = null;
            if (node.getEscape().isPresent()) {
                escape = processWithExceptionHandling(node.getEscape().get(), context);

                if (escape == null) {
                    return null;
                }
            }

            if (value instanceof Slice &&
                    pattern instanceof Slice &&
                    (escape == null || escape instanceof Slice)) {
                JoniRegexp regex;
                if (escape == null) {
                    regex = LikeFunctions.compileLikePattern((Slice) pattern);
                }
                else {
                    regex = LikeFunctions.likePattern((Slice) pattern, (Slice) escape);
                }

                return evaluateLikePredicate(node, (Slice) value, regex);
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (pattern instanceof Slice && (escape == null || escape instanceof Slice) && !isLikePattern((Slice) pattern, Optional.ofNullable((Slice) escape))) {
                Type valueType = type(node.getValue());
                Slice unescapedPattern = unescapeLiteralLikePattern((Slice) pattern, Optional.ofNullable((Slice) escape));
                VarcharType patternType = createVarcharType(countCodePoints(unescapedPattern));

                Expression valueExpression;
                Expression patternExpression;
                if (valueType instanceof CharType) {
                    if (((CharType) valueType).getLength() != patternType.getBoundedLength()) {
                        return false;
                    }
                    valueExpression = toExpression(value, valueType);
                    patternExpression = toExpression(trimTrailingSpaces(unescapedPattern), valueType);
                }
                else if (valueType instanceof VarcharType) {
                    Type superType = typeCoercion.getCommonSuperType(valueType, patternType)
                            .orElseThrow(() -> new IllegalArgumentException("Missing super type when optimizing " + node));
                    valueExpression = toExpression(value, valueType);
                    if (!valueType.equals(superType)) {
                        valueExpression = new Cast(valueExpression, toSqlType(superType), false, typeCoercion.isTypeOnlyCoercion(valueType, superType));
                    }
                    patternExpression = toExpression(unescapedPattern, superType);
                }
                else {
                    throw new IllegalStateException("Unsupported valueType for LIKE: " + valueType);
                }
                return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, valueExpression, patternExpression);
            }

            Optional<Expression> optimizedEscape = Optional.empty();
            if (node.getEscape().isPresent()) {
                optimizedEscape = Optional.of(toExpression(escape, type(node.getEscape().get())));
            }

            return new LikePredicate(
                    toExpression(value, type(node.getValue())),
                    toExpression(pattern, type(node.getPattern())),
                    optimizedEscape);
        }

        private boolean evaluateLikePredicate(LikePredicate node, Slice value, JoniRegexp regex)
        {
            if (type(node.getValue()) instanceof VarcharType) {
                return LikeFunctions.likeVarchar(value, regex);
            }

            Type type = type(node.getValue());
            checkState(type instanceof CharType, "LIKE value is neither VARCHAR or CHAR");
            return LikeFunctions.likeChar((long) ((CharType) type).getLength(), value, regex);
        }

        private JoniRegexp getConstantPattern(LikePredicate node)
        {
            JoniRegexp result = likePatternCache.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();

                if (node.getEscape().isPresent()) {
                    Slice escape = Slices.utf8Slice(((StringLiteral) node.getEscape().get()).getValue());
                    result = LikeFunctions.likePattern(Slices.utf8Slice(pattern.getValue()), escape);
                }
                else {
                    result = LikeFunctions.compileLikePattern(Slices.utf8Slice(pattern.getValue()));
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
            if (value instanceof Expression) {
                if (targetType.equals(sourceType)) {
                    return value;
                }

                return new Cast((Expression) value, node.getType(), node.isSafe(), node.isTypeOnly());
            }

            if (node.isTypeOnly()) {
                return value;
            }

            if (value == null) {
                return null;
            }

            ResolvedFunction operator = metadata.getCoercion(session, sourceType, targetType);

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
        protected Object visitArrayConstructor(ArrayConstructor node, Object context)
        {
            Type elementType = ((ArrayType) type(node)).getElementType();
            BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, node.getValues().size());

            for (Expression expression : node.getValues()) {
                Object value = processWithExceptionHandling(expression, context);
                if (value instanceof Expression) {
                    checkCondition(node.getValues().size() <= 254, NOT_SUPPORTED, "Too many arguments for array constructor");
                    return visitFunctionCall(
                            FunctionCallBuilder.resolve(session, metadata)
                                    .setName(QualifiedName.of(ArrayConstructor.ARRAY_CONSTRUCTOR))
                                    .setArguments(types(node.getValues()), node.getValues())
                                    .build(),
                            context);
                }
                writeNativeValue(elementType, arrayBlockBuilder, value);
            }

            return arrayBlockBuilder.build();
        }

        @Override
        protected Object visitCurrentCatalog(CurrentCatalog node, Object context)
        {
            return visitFunctionCall(desugarCurrentCatalog(session, node, metadata), context);
        }

        @Override
        protected Object visitCurrentSchema(CurrentSchema node, Object context)
        {
            return visitFunctionCall(desugarCurrentSchema(session, node, metadata), context);
        }

        @Override
        protected Object visitCurrentUser(CurrentUser node, Object context)
        {
            return visitFunctionCall(DesugarCurrentUser.getCall(node, metadata, session), context);
        }

        @Override
        protected Object visitCurrentPath(CurrentPath node, Object context)
        {
            return visitFunctionCall(DesugarCurrentPath.getCall(node, metadata, session), context);
        }

        @Override
        protected Object visitRow(Row node, Object context)
        {
            RowType rowType = (RowType) type(node);
            List<Type> parameterTypes = rowType.getTypeParameters();
            List<Expression> arguments = node.getItems();

            int cardinality = arguments.size();
            List<Object> values = new ArrayList<>(cardinality);
            for (Expression argument : arguments) {
                values.add(processWithExceptionHandling(argument, context));
            }
            if (hasUnresolvedValue(values)) {
                return new Row(toExpressions(values, parameterTypes));
            }
            BlockBuilder blockBuilder = new RowBlockBuilder(parameterTypes, null, 1);
            BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
            for (int i = 0; i < cardinality; ++i) {
                writeNativeValue(parameterTypes.get(i), singleRowBlockWriter, values.get(i));
            }
            blockBuilder.closeEntry();
            return rowType.getObject(blockBuilder, 0);
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

            if (hasUnresolvedValue(base, index)) {
                return new SubscriptExpression(toExpression(base, type(node.getBase())), toExpression(index, type(node.getIndex())));
            }

            // Subscript on Row hasn't got a dedicated operator. It is interpreted by hand.
            if (base instanceof SingleRowBlock) {
                SingleRowBlock row = (SingleRowBlock) base;
                int position = toIntExact((long) index - 1);
                if (position < 0 || position >= row.getPositionCount()) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "ROW index out of bounds: " + (position + 1));
                }
                Type returnType = type(node.getBase()).getTypeParameters().get(position);
                return readNativeValue(returnType, row, position);
            }

            // Subscript on Array or Map is interpreted using operator.
            return invokeOperator(OperatorType.SUBSCRIPT, types(node.getBase(), node.getIndex()), ImmutableList.of(base, index));
        }

        @Override
        protected Object visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Object context)
        {
            if (!optimize) {
                throw new UnsupportedOperationException("QuantifiedComparison not yet implemented");
            }
            return node;
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

        private List<Type> types(List<Expression> expressions)
        {
            return expressions.stream()
                    .map(NodeRef::of)
                    .map(expressionTypes::get)
                    .collect(toImmutableList());
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(Expression.class));
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            ResolvedFunction operator = metadata.resolveOperator(session, operatorType, argumentTypes);
            return functionInvoker.invoke(operator, connectorSession, argumentValues);
        }

        private Expression toExpression(Object base, Type type)
        {
            return literalEncoder.toExpression(session, base, type);
        }

        private List<Expression> toExpressions(List<Object> values, List<Type> types)
        {
            return literalEncoder.toExpressions(session, values, types);
        }
    }

    private interface PagePositionContext
    {
        Block getBlock(int channel);

        int getPosition(int channel);
    }

    private static class NoPagePositionContext
            implements PagePositionContext
    {
        @Override
        public Block getBlock(int channel)
        {
            throw new IllegalArgumentException("Context does not contain any blocks");
        }

        @Override
        public int getPosition(int channel)
        {
            throw new IllegalArgumentException("Context does not have a position");
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
