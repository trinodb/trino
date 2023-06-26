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
import io.trino.metadata.Metadata;
import io.trino.spi.StandardErrorCode;
import io.trino.sql.planner.ScopeAware;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentTime;
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
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
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
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.FreeLambdaReferenceExtractor.hasFreeReferencesToLambdaArgument;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.isFieldFromScope;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
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
    private final Metadata metadata;
    private final Analysis analysis;

    private final Scope sourceScope;
    private final Optional<Scope> orderByScope;

    public static void verifySourceAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            List<Expression> expressions,
            Session session,
            Metadata metadata,
            Analysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), session, metadata, analysis);
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
            Metadata metadata,
            Analysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.of(orderByScope), session, metadata, analysis);
        for (Expression expression : expressions) {
            analyzer.analyze(expression);
        }
    }

    private AggregationAnalyzer(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Optional<Scope> orderByScope,
            Session session,
            Metadata metadata,
            Analysis analysis)
    {
        requireNonNull(groupByExpressions, "groupByExpressions is null");
        requireNonNull(sourceScope, "sourceScope is null");
        requireNonNull(orderByScope, "orderByScope is null");
        requireNonNull(session, "session is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(analysis, "analysis is null");

        this.sourceScope = sourceScope;
        this.orderByScope = orderByScope;
        this.session = session;
        this.metadata = metadata;
        this.analysis = analysis;
        this.expressions = groupByExpressions.stream()
                .map(expression -> scopeAwareKey(expression, analysis, sourceScope))
                .collect(toImmutableSet());

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
                    "Grouping field %s should originate from %s", fieldId, sourceScope.getRelationType());
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
            extends AstVisitor<Boolean, Void>
    {
        @Override
        protected Boolean visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("aggregation analysis not yet implemented for: " + node.getClass().getName());
        }

        @Override
        protected Boolean visitAtTimeZone(AtTimeZone node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitSubqueryExpression(SubqueryExpression node, Void context)
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
                        throw semanticException(EXPRESSION_NOT_AGGREGATE, expression,
                                "Subquery uses '%s' which must appear in GROUP BY clause", expression);
                    });

            return true;
        }

        @Override
        protected Boolean visitExists(ExistsPredicate node, Void context)
        {
            checkState(node.getSubquery() instanceof SubqueryExpression);
            return process(node.getSubquery(), context);
        }

        @Override
        protected Boolean visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return process(node.getBase(), context) &&
                    process(node.getIndex(), context);
        }

        @Override
        protected Boolean visitArray(Array node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitCast(Cast node, Void context)
        {
            return process(node.getExpression(), context);
        }

        @Override
        protected Boolean visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return node.getOperands().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitNullIfExpression(NullIfExpression node, Void context)
        {
            return process(node.getFirst(), context) && process(node.getSecond(), context);
        }

        @Override
        protected Boolean visitExtract(Extract node, Void context)
        {
            return process(node.getExpression(), context);
        }

        @Override
        protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return process(node.getMin(), context) &&
                    process(node.getValue(), context) &&
                    process(node.getMax(), context);
        }

        @Override
        protected Boolean visitCurrentTime(CurrentTime node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitLiteral(Literal node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLikePredicate(LikePredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getPattern(), context);
        }

        @Override
        protected Boolean visitInListExpression(InListExpression node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitInPredicate(InPredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getValueList(), context);
        }

        @Override
        protected Boolean visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            return process(node.getValue(), context) && process(node.getSubquery(), context);
        }

        @Override
        protected Boolean visitTrim(Trim node, Void context)
        {
            return process(node.getTrimSource(), context) && (node.getTrimCharacter().isEmpty() || process(node.getTrimCharacter().get(), context));
        }

        @Override
        protected Boolean visitFormat(Format node, Void context)
        {
            return node.getArguments().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitFunctionCall(FunctionCall node, Void context)
        {
            if (metadata.isAggregationFunction(session, node.getName())) {
                if (node.getWindow().isEmpty()) {
                    List<FunctionCall> aggregateFunctions = extractAggregateFunctions(node.getArguments(), session, metadata);
                    List<Expression> windowExpressions = extractWindowExpressions(node.getArguments());

                    if (!aggregateFunctions.isEmpty()) {
                        throw semanticException(NESTED_AGGREGATION,
                                node,
                                "Cannot nest aggregations inside aggregation '%s': %s",
                                node.getName(),
                                aggregateFunctions);
                    }

                    if (!windowExpressions.isEmpty()) {
                        throw semanticException(NESTED_WINDOW,
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
                            List<FieldId> fieldIds = node.getArguments().stream()
                                    .map(NodeRef::of)
                                    .map(columnReferences::get)
                                    .filter(Objects::nonNull)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableList());
                            for (Expression sortKey : sortKeys) {
                                if (!node.getArguments().contains(sortKey)) {
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
                        node.getArguments()
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
                    throw semanticException(FUNCTION_NOT_AGGREGATE,
                            node,
                            "Filter is only valid for aggregation functions",
                            node);
                }
                if (node.getOrderBy().isPresent()) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "ORDER BY is only valid for aggregation functions");
                }
            }

            if (node.getWindow().isPresent()) {
                Window window = node.getWindow().get();
                if (window instanceof WindowSpecification && !process((WindowSpecification) window, context)) {
                    return false;
                }
            }

            return node.getArguments().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitWindowOperation(WindowOperation node, Void context)
        {
            return node.getWindow() instanceof WindowReference || process((WindowSpecification) node.getWindow(), context);
        }

        @Override
        protected Boolean visitLambdaExpression(LambdaExpression node, Void context)
        {
            return process(node.getBody(), context);
        }

        @Override
        protected Boolean visitBindExpression(BindExpression node, Void context)
        {
            for (Expression value : node.getValues()) {
                if (!process(value, context)) {
                    return false;
                }
            }
            return process(node.getFunction(), context);
        }

        @Override
        protected Boolean visitWindowSpecification(WindowSpecification node, Void context)
        {
            for (Expression expression : node.getPartitionBy()) {
                if (!process(expression, context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE,
                            expression,
                            "PARTITION BY expression '%s' must be an aggregate expression or appear in GROUP BY clause",
                            expression);
                }
            }

            for (SortItem sortItem : getSortItemsFromOrderBy(node.getOrderBy())) {
                Expression expression = sortItem.getSortKey();
                if (!process(expression, context)) {
                    throw semanticException(EXPRESSION_NOT_AGGREGATE,
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

        @Override
        protected Boolean visitWindowFrame(WindowFrame node, Void context)
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

        @Override
        protected Boolean visitIdentifier(Identifier node, Void context)
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

        @Override
        protected Boolean visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            ExpressionAnalyzer.LabelPrefixedReference labelDereference = analysis.getLabelDereference(node);
            if (labelDereference != null) {
                return labelDereference.getColumn().map(this::process).orElse(true);
            }

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

        @Override
        protected Boolean visitFieldReference(FieldReference node, Void context)
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

        @Override
        protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitNotExpression(NotExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLogicalExpression(LogicalExpression node, Void context)
        {
            return node.getTerms().stream()
                    .allMatch(item -> process(item, context));
        }

        @Override
        protected Boolean visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.<Expression>builder()
                    .add(node.getCondition())
                    .add(node.getTrueValue());

            if (node.getFalseValue().isPresent()) {
                expressions.add(node.getFalseValue().get());
            }

            return expressions.build().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            if (!process(node.getOperand(), context)) {
                return false;
            }

            for (WhenClause whenClause : node.getWhenClauses()) {
                if (!process(whenClause.getOperand(), context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            if (node.getDefaultValue().isPresent() && !process(node.getDefaultValue().get(), context)) {
                return false;
            }

            return true;
        }

        @Override
        protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                if (!process(whenClause.getOperand(), context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            return node.getDefaultValue().isEmpty() || process(node.getDefaultValue().get(), context);
        }

        @Override
        protected Boolean visitTryExpression(TryExpression node, Void context)
        {
            return process(node.getInnerExpression(), context);
        }

        @Override
        protected Boolean visitRow(Row node, Void context)
        {
            return node.getItems().stream()
                    .allMatch(item -> process(item, context));
        }

        @Override
        protected Boolean visitParameter(Parameter node, Void context)
        {
            if (analysis.isDescribe()) {
                return true;
            }
            Map<NodeRef<Parameter>, Expression> parameters = analysis.getParameters();
            checkArgument(node.getId() < parameters.size(), "Invalid parameter number %s, max values is %s", node.getId(), parameters.size() - 1);
            return process(parameters.get(NodeRef.of(node)), context);
        }

        @Override
        protected Boolean visitGroupingOperation(GroupingOperation node, Void context)
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

        @Override
        protected Boolean visitJsonExists(JsonExists node, Void context)
        {
            return process(node.getJsonPathInvocation(), context);
        }

        @Override
        protected Boolean visitJsonValue(JsonValue node, Void context)
        {
            return process(node.getJsonPathInvocation(), context) &&
                    node.getEmptyDefault().map(expression -> process(expression, context)).orElse(true) &&
                    node.getErrorDefault().map(expression -> process(expression, context)).orElse(true);
        }

        @Override
        protected Boolean visitJsonQuery(JsonQuery node, Void context)
        {
            return process(node.getJsonPathInvocation(), context);
        }

        @Override
        protected Boolean visitJsonPathInvocation(JsonPathInvocation node, Void context)
        {
            return process(node.getInputExpression(), context) &&
                    node.getPathParameters().stream()
                            .map(JsonPathParameter::getParameter)
                            .allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitJsonObject(JsonObject node, Void context)
        {
            return node.getMembers().stream()
                    .allMatch(member -> process(member.getKey(), context) && process(member.getValue(), context));
        }

        @Override
        protected Boolean visitJsonArray(JsonArray node, Void context)
        {
            return node.getElements().stream()
                    .allMatch(element -> process(element.getValue(), context));
        }

        @Override
        public Boolean process(Node node, @Nullable Void context)
        {
            if (node instanceof Expression
                    && expressions.contains(scopeAwareKey(node, analysis, sourceScope))
                    && (orderByScope.isEmpty() || !hasOrderByReferencesToOutputColumns(node))
                    && !hasFreeReferencesToLambdaArgument(node, analysis)) {
                return true;
            }

            return super.process(node, context);
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
                    throw semanticException(errorCode, expression, errorString);
                });
    }
}
