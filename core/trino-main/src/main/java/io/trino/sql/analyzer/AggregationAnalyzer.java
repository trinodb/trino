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
import io.trino.Session;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AccessControl;
import io.trino.spi.StandardErrorCode;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ScopeAware;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.ArrayWildcardSubscript;
import io.trino.sql.tree.AtLocal;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.IntervalValueExpression;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonConstructor;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonSerialize;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.MethodCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.Overlay;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StaticMethodCall;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.UniquePredicate;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NESTED_AGGREGATION;
import static io.trino.spi.StandardErrorCode.NESTED_WINDOW;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.FreeLambdaReferenceExtractor.hasFreeReferencesToLambdaArgument;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.isFieldFromScope;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.tree.AstTraversal.forEachChild;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Checks whether an expression is constant with respect to the group
 */
class AggregationAnalyzer
{
    // fields and expressions in the group by clause
    private final Set<FieldId> groupingFields;
    private final Set<ScopeAware<Expression>> expressions;
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences;

    private final Session session;
    private final Analysis analysis;
    private final FunctionResolver functionResolver;
    private final AccessControl accessControl;

    private final Scope sourceScope;
    private final Optional<Scope> orderByScope;

    public static void verifySourceAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            List<Expression> expressions,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Analysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), session, plannerContext, accessControl, analysis);
        for (Expression expression : expressions) {
            analyzer.analyze(expression);
        }
    }

    public static void verifyOrderByAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Scope orderByScope,
            List<Expression> expressions,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Analysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.of(orderByScope), session, plannerContext, accessControl, analysis);
        for (Expression expression : expressions) {
            analyzer.analyze(expression);
        }
    }

    private AggregationAnalyzer(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Optional<Scope> orderByScope,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Analysis analysis)
    {
        requireNonNull(groupByExpressions, "groupByExpressions is null");
        requireNonNull(sourceScope, "sourceScope is null");
        requireNonNull(orderByScope, "orderByScope is null");
        requireNonNull(session, "session is null");
        requireNonNull(plannerContext, "metadata is null");
        requireNonNull(accessControl, "accessControl is null");
        requireNonNull(analysis, "analysis is null");

        this.sourceScope = sourceScope;
        this.orderByScope = orderByScope;
        this.session = session;
        this.analysis = analysis;
        this.accessControl = accessControl;
        this.expressions = groupByExpressions.stream()
                .map(expression -> scopeAwareKey(expression, analysis, sourceScope))
                .collect(toImmutableSet());
        functionResolver = plannerContext.getFunctionResolver();

        // No defensive copy here for performance reasons.
        // Copying this map may lead to quadratic time complexity
        this.columnReferences = analysis.getColumnReferenceFields();

        this.groupingFields = groupByExpressions.stream()
                .map(NodeRef::of)
                .filter(columnReferences::containsKey)
                .map(columnReferences::get)
                .map(ResolvedField::getFieldId)
                .collect(toImmutableSet());

        this.groupingFields.forEach(fieldId -> {
            checkState(isFieldFromScope(fieldId, sourceScope),
                    "Grouping field %s should originate from %s",
                    fieldId,
                    sourceScope.getRelationType());
        });
    }

    private void analyze(Expression expression)
    {
        Visitor visitor = new Visitor();
        if (!visitor.process(expression, null)) {
            throw semanticException(EXPRESSION_NOT_AGGREGATE, expression, "'%s' must be an aggregate expression or appear in GROUP BY clause", expression);
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class Visitor
    {
        private Boolean dispatch(Node root, Void context)
        {
            return switch (root) {
                case AllRows node -> visitExpression(node);
                case ArithmeticBinaryExpression node -> process(node.getLeft(), context) && process(node.getRight(), context);
                case ArithmeticUnaryExpression node -> process(node.getValue(), context);
                case Array node -> node.getValues().stream().allMatch(expression -> process(expression, context));
                case ArrayWildcardSubscript node -> visitExpression(node);
                case AtLocal node -> process(node.getValue(), context);
                case AtTimeZone node -> process(node.getValue(), context);
                case Cast node -> process(node.getExpression(), context);
                case CoalesceExpression node -> node.getOperands().stream().allMatch(expression -> process(expression, context));
                case CurrentCatalog _,
                     CurrentDate _,
                     CurrentPath _,
                     CurrentSchema _,
                     CurrentTime _,
                     CurrentTimestamp _,
                     CurrentUser _ -> true;
                case DataType node -> visitExpression(node);
                case DereferenceExpression node -> visitDereferenceExpression(node, context);
                case ExistsPredicate node -> visitExists(node, context);
                case Extract node -> process(node.getExpression(), context);
                case FieldReference node -> visitFieldReference(node);
                case Format node -> node.getArguments().stream().allMatch(expression -> process(expression, context));
                case FunctionCall node -> visitFunctionCall(node, context);
                case GroupingOperation node -> visitGroupingOperation(node);
                case Identifier node -> visitIdentifier(node);
                case IfExpression node -> visitIfExpression(node, context);
                case InListExpression node -> node.getValues().stream().allMatch(expression -> process(expression, context));
                case IntervalValueExpression node -> process(node.getLeft(), context) && process(node.getRight(), context);
                case JsonArray node -> visitJsonArray(node, context);
                case JsonConstructor node -> process(node.getExpression(), context);
                case JsonExists node -> process(node.getJsonPathInvocation(), context);
                case JsonObject node -> visitJsonObject(node, context);
                case JsonPathInvocation node -> visitJsonPathInvocation(node, context);
                case JsonQuery node -> process(node.getJsonPathInvocation(), context);
                case JsonSerialize node -> process(node.getExpression(), context);
                case JsonValue node -> visitJsonValue(node, context);
                case LambdaArgumentDeclaration node -> visitExpression(node);
                case LambdaExpression node -> process(node.getBody(), context);
                case Literal _,
                     LocalTime _,
                     LocalTimestamp _ -> true;
                case LogicalExpression node -> visitLogicalExpression(node, context);
                case MethodCall node -> visitExpression(node);
                case NotExpression node -> process(node.getValue(), context);
                case NullIfExpression node -> process(node.getFirst(), context) && process(node.getSecond(), context);
                case Overlay node -> visitExpression(node);
                case Parameter node -> visitParameter(node, context);
                case Predicated node -> visitPredicated(node, context);
                case Row node -> visitRow(node, context);
                case Row.Field node -> process(node.getExpression(), context);
                case SearchedCaseExpression node -> visitSearchedCaseExpression(node, context);
                case SimpleCaseExpression node -> visitSimpleCaseExpression(node, context);
                case StaticMethodCall node -> visitExpression(node);
                case SubqueryExpression node -> visitSubqueryExpression(node);
                case SubscriptExpression node -> visitSubscriptExpression(node, context);
                case Trim node -> visitTrim(node, context);
                case TryExpression node -> process(node.getInnerExpression(), context);
                case UniquePredicate node -> visitExpression(node);
                case WhenClause node -> visitExpression(node);
                case WindowFrame node -> visitWindowFrame(node, context);
                case WindowOperation node -> visitWindowOperation(node, context);
                case WindowSpecification node -> visitWindowSpecification(node, context);
                default -> null;
            };
        }

        private Boolean visitExpression(Expression node)
        {
            throw new UnsupportedOperationException("aggregation analysis not yet implemented for: " + node.getClass().getName());
        }

        private Boolean visitSubqueryExpression(SubqueryExpression node)
        {
            /*
             * Column reference can resolve to (a) some subquery's scope, (b) a projection (ORDER BY scope),
             * (c) source scope or (d) outer query scope (effectively a constant).
             * From AggregationAnalyzer's perspective, only case (c) needs verification.
             */
            getReferencesToScope(node, analysis, sourceScope)
                    .filter(expression -> !isGroupingKey(expression))
                    .findFirst()
                    .ifPresent(expression -> {
                        throw semanticException(
                                EXPRESSION_NOT_AGGREGATE,
                                expression,
                                "Subquery uses '%s' which must appear in GROUP BY clause",
                                expression);
                    });

            return true;
        }

        private Boolean visitExists(ExistsPredicate node, Void context)
        {
            checkState(node.getSubquery() instanceof SubqueryExpression);
            return process(node.getSubquery(), context);
        }

        private Boolean visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return process(node.getBase(), context) &&
                    process(node.getIndex(), context);
        }

        private Boolean visitPredicated(Predicated node, Void context)
        {
            if (!process(node.getValue(), context)) {
                return false;
            }
            for (Node child : node.getPredicate().getChildren()) {
                if (child instanceof Expression expression && !process(expression, context)) {
                    return false;
                }
            }
            return true;
        }

        private Boolean visitTrim(Trim node, Void context)
        {
            return process(node.getTrimSource(), context) && (node.getTrimCharacter().isEmpty() || process(node.getTrimCharacter().get(), context));
        }

        private Boolean visitFunctionCall(FunctionCall node, Void context)
        {
            if (functionResolver.isAggregationFunction(session, node.getName(), accessControl)) {
                if (node.getWindow().isEmpty()) {
                    List<Expression> arguments = node.argumentValues();
                    List<FunctionCall> aggregateFunctions = extractAggregateFunctions(arguments, session, functionResolver, accessControl);
                    List<Expression> windowExpressions = extractWindowExpressions(arguments);

                    if (!aggregateFunctions.isEmpty()) {
                        throw semanticException(
                                NESTED_AGGREGATION,
                                node,
                                "Cannot nest aggregations inside aggregation '%s': %s",
                                node.getName(),
                                aggregateFunctions);
                    }

                    if (!windowExpressions.isEmpty()) {
                        throw semanticException(
                                NESTED_WINDOW,
                                node,
                                "Cannot nest window functions or row pattern measures inside aggregation '%s': %s",
                                node.getName(),
                                windowExpressions);
                    }

                    if (node.getOrderBy().isPresent()) {
                        List<Expression> sortKeys = node.getOrderBy().get().getSortItems().stream()
                                .map(SortItem::getSortKey)
                                .collect(toImmutableList());
                        if (node.isDistinct()) {
                            List<FieldId> fieldIds = arguments.stream()
                                    .map(NodeRef::of)
                                    .map(columnReferences::get)
                                    .filter(Objects::nonNull)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableList());
                            for (Expression sortKey : sortKeys) {
                                if (!arguments.contains(sortKey)) {
                                    ResolvedField field = columnReferences.get(NodeRef.of(sortKey));
                                    if (field == null || !fieldIds.contains(field.getFieldId())) {
                                        throw semanticException(
                                                EXPRESSION_NOT_IN_DISTINCT,
                                                sortKey,
                                                "For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");
                                    }
                                }
                            }
                        }
                        // in case of aggregate function in ORDER BY, ensure that no output fields are referenced from aggregation's ordering expressions
                        if (orderByScope.isPresent()) {
                            for (Expression sortKey : sortKeys) {
                                verifyNoOrderByReferencesToOutputColumns(
                                        sortKey,
                                        COLUMN_NOT_FOUND,
                                        "ORDER BY clause in aggregation function must not reference query output columns");
                            }
                        }
                    }

                    // in case of aggregate function in ORDER BY, ensure that no output fields are referenced from aggregation's arguments or filter
                    if (orderByScope.isPresent()) {
                        arguments
                                .forEach(argument -> verifyNoOrderByReferencesToOutputColumns(
                                        argument,
                                        COLUMN_NOT_FOUND,
                                        "Invalid reference to output projection attribute from ORDER BY aggregation"));
                        node.getFilter().ifPresent(expression -> verifyNoOrderByReferencesToOutputColumns(
                                expression,
                                COLUMN_NOT_FOUND,
                                "Invalid reference to output projection attribute from ORDER BY aggregation"));
                    }

                    return true;
                }
            }
            else {
                if (node.getFilter().isPresent()) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "Filter is only valid for aggregation functions");
                }
                if (node.getOrderBy().isPresent()) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "ORDER BY is only valid for aggregation functions");
                }
            }

            if (node.getWindow().isPresent()) {
                Window window = node.getWindow().get();
                if (window instanceof WindowSpecification windowSpecification && !process(windowSpecification, context)) {
                    return false;
                }
            }

            return node.argumentValues().stream().allMatch(expression -> process(expression, context));
        }

        private Boolean visitWindowOperation(WindowOperation node, Void context)
        {
            return switch (node.getWindow()) {
                case WindowReference _ -> true;
                case WindowSpecification windowSpecification -> process(windowSpecification, context);
            };
        }

        private Boolean visitWindowSpecification(WindowSpecification node, Void context)
        {
            for (Expression expression : node.getPartitionBy()) {
                if (!process(expression, context)) {
                    throw semanticException(
                            EXPRESSION_NOT_AGGREGATE,
                            expression,
                            "PARTITION BY expression '%s' must be an aggregate expression or appear in GROUP BY clause",
                            expression);
                }
            }

            for (SortItem sortItem : getSortItemsFromOrderBy(node.getOrderBy())) {
                Expression expression = sortItem.getSortKey();
                if (!process(expression, context)) {
                    throw semanticException(
                            EXPRESSION_NOT_AGGREGATE,
                            expression,
                            "ORDER BY expression '%s' must be an aggregate expression or appear in GROUP BY clause",
                            expression);
                }
            }

            if (node.getFrame().isPresent()) {
                process(node.getFrame().get(), context);
            }

            return true;
        }

        private Boolean visitWindowFrame(WindowFrame node, Void context)
        {
            Optional<Expression> start = node.getStart().getValue();
            if (start.isPresent()) {
                if (!process(start.get(), context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE, start.get(), "Window frame start must be an aggregate expression or appear in GROUP BY clause");
                }
            }
            if (node.getEnd().isPresent() && node.getEnd().get().getValue().isPresent()) {
                Expression endValue = node.getEnd().get().getValue().get();
                if (!process(endValue, context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE, endValue, "Window frame end must be an aggregate expression or appear in GROUP BY clause");
                }
            }
            for (MeasureDefinition measure : node.getMeasures()) {
                if (!process(measure.getExpression(), context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE, measure, "Row pattern measure must be an aggregate expression or appear in GROUP BY clause");
                }
            }
            for (VariableDefinition variableDefinition : node.getVariableDefinitions()) {
                if (!process(variableDefinition.getExpression(), context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE, variableDefinition, "Row pattern variable definition must be an aggregate expression or appear in GROUP BY clause");
                }
            }

            return true;
        }

        private Boolean visitIdentifier(Identifier node)
        {
            if (analysis.getLambdaArgumentReferences().containsKey(NodeRef.of(node))) {
                return true;
            }

            if (!hasReferencesToScope(node, analysis, sourceScope)) {
                // reference to outer scope is group-invariant
                return true;
            }

            return isGroupingKey(node);
        }

        private Boolean visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (!hasReferencesToScope(node, analysis, sourceScope)) {
                // reference to outer scope is group-invariant
                return true;
            }

            if (columnReferences.containsKey(NodeRef.<Expression>of(node))) {
                return isGroupingKey(node);
            }

            // Allow SELECT col1.f1 FROM table1 GROUP BY col1
            return process(node.getBase(), context);
        }

        private boolean isGroupingKey(Expression node)
        {
            FieldId fieldId = requireNonNull(columnReferences.get(NodeRef.of(node)), () -> "No field for " + node).getFieldId();

            if (orderByScope.isPresent() && isFieldFromScope(fieldId, orderByScope.get())) {
                return true;
            }

            return groupingFields.contains(fieldId);
        }

        private Boolean visitFieldReference(FieldReference node)
        {
            if (orderByScope.isPresent()) {
                return true;
            }

            FieldId fieldId = requireNonNull(columnReferences.get(NodeRef.of(node)), () -> "No field for " + node).getFieldId();
            boolean inGroup = groupingFields.contains(fieldId);
            if (!inGroup) {
                Field field = sourceScope.getRelationType().getFieldByIndex(node.getFieldIndex());

                String column;
                if (field.getName().isEmpty()) {
                    column = Integer.toString(node.getFieldIndex() + 1);
                }
                else if (field.getRelationAlias().isPresent()) {
                    column = format("'%s.%s'", field.getRelationAlias().get(), field.getName().get());
                }
                else {
                    column = "'" + field.getName().get() + "'";
                }

                throw semanticException(EXPRESSION_NOT_AGGREGATE, node, "Column %s not in GROUP BY clause", column);
            }
            return inGroup;
        }

        private Boolean visitLogicalExpression(LogicalExpression node, Void context)
        {
            return node.getTerms().stream()
                    .allMatch(item -> process(item, context));
        }

        private Boolean visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.<Expression>builder()
                    .add(node.getCondition())
                    .add(node.getTrueValue());

            if (node.getFalseValue().isPresent()) {
                expressions.add(node.getFalseValue().get());
            }

            return expressions.build().stream().allMatch(expression -> process(expression, context));
        }

        private Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            if (!process(node.getOperand(), context)) {
                return false;
            }

            for (WhenClause whenClause : node.getWhenClauses()) {
                switch (whenClause.getMatch()) {
                    case WhenClause.Operand operand -> {
                        if (!process(operand.expression(), context)) {
                            return false;
                        }
                    }
                    case WhenClause.Partial partial -> {
                        for (Node child : partial.predicate().getChildren()) {
                            if (child instanceof Expression expression && !process(expression, context)) {
                                return false;
                            }
                        }
                    }
                }
                if (!process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            return node.getDefaultValue().isEmpty() || process(node.getDefaultValue().get(), context);
        }

        private Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                Expression condition = ((WhenClause.Operand) whenClause.getMatch()).expression();
                if (!process(condition, context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            return node.getDefaultValue().isEmpty() || process(node.getDefaultValue().get(), context);
        }

        private Boolean visitRow(Row node, Void context)
        {
            return node.getFields().stream()
                    .allMatch(item -> process(item, context));
        }

        private Boolean visitParameter(Parameter node, Void context)
        {
            if (analysis.isDescribe()) {
                return true;
            }
            Map<NodeRef<Parameter>, Expression> parameters = analysis.getParameters();
            checkArgument(node.getId() < parameters.size(), "Invalid parameter number %s, max values is %s", node.getId(), parameters.size() - 1);
            return process(parameters.get(NodeRef.of(node)), context);
        }

        private Boolean visitGroupingOperation(GroupingOperation node)
        {
            // ensure that no output fields are referenced from ORDER BY clause
            if (orderByScope.isPresent()) {
                node.getGroupingColumns().forEach(groupingColumn -> verifyNoOrderByReferencesToOutputColumns(
                        groupingColumn,
                        INVALID_ARGUMENTS,
                        "Invalid reference to output of SELECT clause from grouping() expression in ORDER BY"));
            }

            Optional<Expression> argumentNotInGroupBy = node.getGroupingColumns().stream()
                    .filter(argument -> !columnReferences.containsKey(NodeRef.of(argument)) || !isGroupingKey(argument))
                    .findAny();
            if (argumentNotInGroupBy.isPresent()) {
                throw semanticException(
                        INVALID_ARGUMENTS,
                        node,
                        "The arguments to GROUPING() must be expressions referenced by the GROUP BY at the associated query level. Mismatch due to %s.",
                        argumentNotInGroupBy.get());
            }
            return true;
        }

        private Boolean visitJsonValue(JsonValue node, Void context)
        {
            return process(node.getJsonPathInvocation(), context) &&
                    node.getEmptyDefault().map(expression -> process(expression, context)).orElse(true) &&
                    node.getErrorDefault().map(expression -> process(expression, context)).orElse(true);
        }

        private Boolean visitJsonPathInvocation(JsonPathInvocation node, Void context)
        {
            return process(node.getInputExpression(), context) &&
                    node.getPathParameters().stream()
                            .map(JsonPathParameter::getParameter)
                            .allMatch(expression -> process(expression, context));
        }

        private Boolean visitJsonObject(JsonObject node, Void context)
        {
            return node.getMembers().stream()
                    .allMatch(member -> process(member.getKey(), context) && process(member.getValue(), context));
        }

        private Boolean visitJsonArray(JsonArray node, Void context)
        {
            return node.getElements().stream()
                    .allMatch(element -> process(element.getValue(), context));
        }

        public Boolean process(Node node, @Nullable Void context)
        {
            if (node instanceof Expression
                    && expressions.contains(scopeAwareKey(node, analysis, sourceScope))
                    && (orderByScope.isEmpty() || !hasOrderByReferencesToOutputColumns(node))
                    && !hasFreeReferencesToLambdaArgument(node, analysis)) {
                return true;
            }

            return dispatch(node, context);
        }
    }

    private boolean hasOrderByReferencesToOutputColumns(Node node)
    {
        return hasReferencesToScope(node, analysis, orderByScope.get());
    }

    private void verifyNoOrderByReferencesToOutputColumns(Node node, StandardErrorCode errorCode, String errorString)
    {
        getReferencesToScope(node, analysis, orderByScope.get())
                .findFirst()
                .ifPresent(expression -> {
                    throw semanticException(errorCode, expression, "%s", errorString);
                });
    }

    public static boolean containsAggregation(Expression expression, Function<FunctionCall, ResolvedFunction> functionResolver)
    {
        requireNonNull(functionResolver, "functionResolver is null");
        requireNonNull(expression, "expression is null");

        AtomicBoolean containsAggregation = new AtomicBoolean(false);
        findAggregation(expression, functionResolver, containsAggregation);
        return containsAggregation.get();
    }

    private static void findAggregation(Node node, Function<FunctionCall, ResolvedFunction> functionResolver, AtomicBoolean containsAggregation)
    {
        switch (node) {
            case SubqueryExpression _ -> {
                // Don't traverse into subqueries within an expression
            }
            case FunctionCall functionCall -> {
                if (functionResolver.apply(functionCall).functionKind() == AGGREGATE) {
                    containsAggregation.set(true);
                    return;
                }
                forEachChild(node, child -> findAggregation(child, functionResolver, containsAggregation));
            }
            default -> forEachChild(node, child -> findAggregation(child, functionResolver, containsAggregation));
        }
    }
}
