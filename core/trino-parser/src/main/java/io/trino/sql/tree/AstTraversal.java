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
package io.trino.sql.tree;

import java.util.function.Consumer;

public final class AstTraversal
{
    private AstTraversal() {}

    /**
     * Invokes the action on each child of the node that a traversal of the
     * tree should descend into. Nodes that are not traversed have no children.
     */
    public static void forEachChild(Node root, Consumer<? super Node> action)
    {
        switch (root) {
            // Nodes without traversed children come first, followed by the most common nodes,
            // as a type switch tests the cases in order
            case Literal _, Identifier _ -> {}
            case LogicalExpression node -> {
                for (Node child : node.getTerms()) {
                    action.accept(child);
                }
            }
            case FunctionCall node -> {
                for (CallArgument argument : node.getArguments()) {
                    action.accept(argument.getValue());
                }

                if (node.getOrderBy().isPresent()) {
                    action.accept(node.getOrderBy().get());
                }

                if (node.getWindow().isPresent()) {
                    action.accept((Node) node.getWindow().get());
                }

                if (node.getFilter().isPresent()) {
                    action.accept(node.getFilter().get());
                }
            }
            case DereferenceExpression node -> {
                action.accept(node.getBase());
            }
            case ArithmeticBinaryExpression node -> {
                action.accept(node.getLeft());
                action.accept(node.getRight());
            }
            case Cast node -> {
                action.accept(node.getExpression());
            }
            case Predicated node -> {
                action.accept(node.getValue());
                action.accept(node.getPredicate());
            }
            case SingleColumn node -> {
                action.accept(node.getExpression());
            }
            case SubqueryExpression node -> {
                action.accept(node.getQuery());
            }
            case InPredicate node -> {
                action.accept(node.getValueList());
            }
            case InListExpression node -> {
                for (Expression value : node.getValues()) {
                    action.accept(value);
                }
            }
            case BetweenPredicate node -> {
                action.accept(node.getMin());
                action.accept(node.getMax());
            }
            case LikePredicate node -> {
                action.accept(node.getPattern());
                node.getEscape().ifPresent(escape -> action.accept(escape));
            }
            case NotExpression node -> {
                action.accept(node.getValue());
            }
            case SearchedCaseExpression node -> {
                for (WhenClause clause : node.getWhenClauses()) {
                    action.accept(clause);
                }
                node.getDefaultValue()
                        .ifPresent(value -> action.accept(value));
            }
            case SimpleCaseExpression node -> {
                action.accept(node.getOperand());
                for (WhenClause clause : node.getWhenClauses()) {
                    action.accept(clause);
                }

                node.getDefaultValue()
                        .ifPresent(value -> action.accept(value));
            }
            case WhenClause node -> {
                action.accept(node.getMatch().node());
                action.accept(node.getResult());
            }
            case CoalesceExpression node -> {
                for (Expression operand : node.getOperands()) {
                    action.accept(operand);
                }
            }
            case ArithmeticUnaryExpression node -> {
                action.accept(node.getValue());
            }
            case IfExpression node -> {
                action.accept(node.getCondition());
                action.accept(node.getTrueValue());
                if (node.getFalseValue().isPresent()) {
                    action.accept(node.getFalseValue().get());
                }
            }
            case Extract node -> {
                action.accept(node.getExpression());
            }
            case Row node -> {
                for (Row.Field field : node.getFields()) {
                    action.accept(field);
                }
            }
            case AtTimeZone node -> {
                action.accept(node.getValue());
                action.accept(node.getTimeZone());
            }
            case AliasedRelation node -> {
                action.accept(node.getRelation());
            }
            case Table node -> {
                node.getQueryPeriod().ifPresent(period -> action.accept(period));
            }
            case Join node -> {
                action.accept(node.getLeft());
                action.accept(node.getRight());

                node.getCriteria()
                        .filter(criteria -> criteria instanceof JoinOn)
                        .ifPresent(criteria -> action.accept(((JoinOn) criteria).getExpression()));
            }
            case QuerySpecification node -> {
                action.accept(node.getSelect());
                if (node.getFrom().isPresent()) {
                    action.accept(node.getFrom().get());
                }
                if (node.getWhere().isPresent()) {
                    action.accept(node.getWhere().get());
                }
                if (node.getGroupBy().isPresent()) {
                    action.accept(node.getGroupBy().get());
                }
                if (node.getHaving().isPresent()) {
                    action.accept(node.getHaving().get());
                }
                for (WindowDefinition windowDefinition : node.getWindows()) {
                    action.accept(windowDefinition);
                }
                if (node.getOrderBy().isPresent()) {
                    action.accept(node.getOrderBy().get());
                }
                if (node.getOffset().isPresent()) {
                    action.accept(node.getOffset().get());
                }
                if (node.getLimit().isPresent()) {
                    action.accept(node.getLimit().get());
                }
            }
            case Select node -> {
                for (SelectItem item : node.getSelectItems()) {
                    action.accept(item);
                }
            }
            case Query node -> {
                if (!node.getSessionProperties().isEmpty()) {
                    for (SessionProperty sessionProperty : node.getSessionProperties()) {
                        action.accept(sessionProperty.getValue());
                    }
                }
                if (node.getWith().isPresent()) {
                    action.accept(node.getWith().get());
                }
                action.accept(node.getQueryBody());
                if (node.getOrderBy().isPresent()) {
                    action.accept(node.getOrderBy().get());
                }
                if (node.getOffset().isPresent()) {
                    action.accept(node.getOffset().get());
                }
                if (node.getLimit().isPresent()) {
                    action.accept(node.getLimit().get());
                }
            }
            case TableSubquery node -> {
                action.accept(node.getQuery());
            }
            case SortItem node -> {
                action.accept(node.getSortKey());
            }
            case AddColumn node -> {
                action.accept(node.getColumn());
            }
            case AllColumns node -> {
                node.getTarget().ifPresent(value -> action.accept(value));
            }
            case Analyze node -> {
                for (Property property : node.getProperties()) {
                    action.accept(property);
                }
            }
            case Array node -> {
                for (Expression expression : node.getValues()) {
                    action.accept(expression);
                }
            }
            case AtLocal node -> {
                action.accept(node.getValue());
            }
            case Call node -> {
                for (CallArgument argument : node.getArguments()) {
                    action.accept(argument.getValue());
                }
            }
            case ComparisonPredicate node -> {
                action.accept(node.getRight());
            }
            case CreateSchema node -> {
                for (Property property : node.getProperties()) {
                    action.accept(property);
                }
            }
            case CreateTable node -> {
                for (TableElement tableElement : node.getElements()) {
                    action.accept(tableElement);
                }
                for (Property property : node.getProperties()) {
                    action.accept(property);
                }
            }
            case CreateTableAsSelect node -> {
                action.accept(node.getQuery());
                for (Property property : node.getProperties()) {
                    action.accept(property);
                }
            }
            case CreateView node -> {
                action.accept(node.getQuery());
            }
            case Delete node -> {
                action.accept(node.getTable());
                node.getWhere().ifPresent(where -> action.accept(where));
            }
            case DistinctFromPredicate node -> {
                action.accept(node.getRight());
            }
            case ExcludedPattern node -> {
                action.accept(node.getPattern());
            }
            case ExistsPredicate node -> {
                action.accept(node.getSubquery());
            }
            case Explain node -> {
                action.accept(node.getStatement());

                for (ExplainOption option : node.getOptions()) {
                    action.accept(option);
                }
            }
            case ExplainAnalyze node -> {
                action.accept(node.getStatement());
            }
            case FetchFirst node -> {
                node.getRowCount().ifPresent(action);
            }
            case Format node -> {
                for (Expression argument : node.getArguments()) {
                    action.accept(argument);
                }
            }
            case FrameBound node -> {
                if (node.getValue().isPresent()) {
                    action.accept(node.getValue().get());
                }
            }
            case GroupBy node -> {
                for (GroupingElement groupingElement : node.getGroupingElements()) {
                    action.accept(groupingElement);
                }
            }
            case GroupingOperation node -> {
                for (Expression columnArgument : node.getGroupingColumns()) {
                    action.accept(columnArgument);
                }
            }
            case Insert node -> {
                action.accept(node.getQuery());
            }
            case IntervalValueExpression node -> {
                action.accept(node.getLeft());
                action.accept(node.getRight());
            }
            case JsonArray node -> {
                for (JsonArrayElement element : node.getElements()) {
                    action.accept(element);
                }
            }
            case JsonArrayElement node -> {
                action.accept(node.getValue());
            }
            case JsonConstructor node -> {
                action.accept(node.getExpression());
            }
            case JsonExists node -> {
                action.accept(node.getJsonPathInvocation());
            }
            case JsonObject node -> {
                for (JsonObjectMember member : node.getMembers()) {
                    action.accept(member);
                }
            }
            case JsonObjectMember node -> {
                action.accept(node.getKey());
                action.accept(node.getValue());
            }
            case JsonPathInvocation node -> {
                action.accept(node.getInputExpression());
                for (JsonPathParameter parameter : node.getPathParameters()) {
                    action.accept(parameter.getParameter());
                }
            }
            case JsonQuery node -> {
                action.accept(node.getJsonPathInvocation());
            }
            case JsonSerialize node -> {
                action.accept(node.getExpression());
            }
            case JsonTable node -> {
                action.accept(node.getJsonPathInvocation());
                for (JsonTableColumnDefinition column : node.getColumns()) {
                    action.accept(column);
                }
            }
            case JsonValue node -> {
                action.accept(node.getJsonPathInvocation());
                node.getEmptyDefault().ifPresent(expression -> action.accept(expression));
                node.getErrorDefault().ifPresent(expression -> action.accept(expression));
            }
            case LambdaExpression node -> {
                action.accept(node.getBody());
            }
            case Lateral node -> {
                action.accept(node.getQuery());
            }
            case Limit node -> {
                action.accept(node.getRowCount());
            }
            case MatchPredicate node -> {
                action.accept(node.getSubquery());
            }
            case MeasureDefinition node -> {
                action.accept(node.getExpression());
            }
            case Merge node -> {
                action.accept(node.getTarget());
                action.accept(node.getSource());
                action.accept(node.getPredicate());
                node.getMergeCases().forEach(mergeCase -> action.accept(mergeCase));
            }
            case MergeDelete node -> {
                node.getExpression().ifPresent(expression -> action.accept(expression));
            }
            case MergeInsert node -> {
                node.getExpression().ifPresent(expression -> action.accept(expression));
                node.getColumns().forEach(column -> action.accept(column));
                node.getValues().forEach(expression -> action.accept(expression));
            }
            case MergeUpdate node -> {
                node.getExpression().ifPresent(expression -> action.accept(expression));
                node.getAssignments().forEach(assignment -> {
                    action.accept(assignment.getTarget());
                    action.accept(assignment.getValue());
                });
            }
            case MethodCall node -> {
                action.accept(node.getReceiver());
                for (CallArgument argument : node.getArguments()) {
                    action.accept(argument.getValue());
                }
            }
            case Nearest node -> {
                action.accept(node.getRelation());
                node.getWhere().ifPresent(expression -> action.accept(expression));
                action.accept(node.getMatch());
            }
            case NestedColumns node -> {
                for (JsonTableColumnDefinition column : node.getColumns()) {
                    action.accept(column);
                }
            }
            case NullIfExpression node -> {
                action.accept(node.getFirst());
                action.accept(node.getSecond());
            }
            case Offset node -> {
                action.accept(node.getRowCount());
            }
            case OrderBy node -> {
                for (SortItem sortItem : node.getSortItems()) {
                    action.accept(sortItem);
                }
            }
            case OverlapsPredicate node -> {
                action.accept(node.getRight());
            }
            case Overlay node -> {
                action.accept(node.getValue());
                action.accept(node.getReplacement());
                action.accept(node.getStart());
                node.getLength().ifPresent(length -> action.accept(length));
            }
            case PatternAlternation node -> {
                for (RowPattern rowPattern : node.getPatterns()) {
                    action.accept(rowPattern);
                }
            }
            case PatternConcatenation node -> {
                for (RowPattern rowPattern : node.getPatterns()) {
                    action.accept(rowPattern);
                }
            }
            case PatternPermutation node -> {
                for (RowPattern rowPattern : node.getPatterns()) {
                    action.accept(rowPattern);
                }
            }
            case PatternVariable node -> {
                action.accept(node.getName());
            }
            case Pivot node -> {
                action.accept(node.getInput());
                for (PivotAggregation aggregation : node.getAggregations()) {
                    action.accept(aggregation);
                }
                for (Expression pivotColumn : node.getPivotColumns()) {
                    action.accept(pivotColumn);
                }
                for (PivotValueGroup valueGroup : node.getValueGroups()) {
                    action.accept(valueGroup);
                }
                node.getGroupBy().ifPresent(groupBy -> action.accept(groupBy));
            }
            case PivotAggregation node -> {
                action.accept(node.getExpression());
            }
            case PivotValueGroup node -> {
                for (Expression value : node.getValues()) {
                    action.accept(value);
                }
            }
            case Property node -> {
                action.accept(node.getName());
                if (!node.isSetToDefault()) {
                    action.accept(node.getNonDefaultValue());
                }
            }
            case QuantifiedComparisonPredicate node -> {
                action.accept(node.getSubquery());
            }
            case QuantifiedPattern node -> {
                action.accept(node.getPattern());
            }
            case QueryPeriod node -> {
                node.getStart().ifPresent(start -> action.accept(start));
                node.getEnd().ifPresent(end -> action.accept(end));
            }
            case Row.Field node -> {
                node.getName().ifPresent(name -> action.accept(name));
                action.accept(node.getExpression());
            }
            case SampledRelation node -> {
                action.accept(node.getRelation());
                action.accept(node.getSamplePercentage());
            }
            case SetOperation node -> {
                for (Relation relation : node.getRelations()) {
                    action.accept(relation);
                }
            }
            case SetSession node -> {
                action.accept(node.getValue());
            }
            case ShowStats node -> {
                action.accept(node.getRelation());
            }
            case SimpleGroupBy node -> {
                for (Expression expression : node.getExpressions()) {
                    action.accept(expression);
                }
            }
            case StartTransaction node -> {
                for (TransactionMode transactionMode : node.getTransactionModes()) {
                    action.accept(transactionMode);
                }
            }
            case StaticMethodCall node -> {
                for (CallArgument argument : node.getArguments()) {
                    action.accept(argument.getValue());
                }
            }
            case SubscriptExpression node -> {
                action.accept(node.getBase());
                action.accept(node.getIndex());
            }
            case TableFunctionInvocation node -> {
                for (TableFunctionArgument argument : node.getArguments()) {
                    action.accept(argument.getValue());
                }
            }
            case Trim node -> {
                action.accept(node.getTrimSource());
                node.getTrimCharacter().ifPresent(trimChar -> action.accept(trimChar));
            }
            case TryExpression node -> {
                action.accept(node.getInnerExpression());
            }
            case UniquePredicate node -> {
                action.accept(node.getSubquery());
            }
            case Unnest node -> {
                for (Expression expression : node.getExpressions()) {
                    action.accept(expression);
                }
            }
            case Update node -> {
                action.accept(node.getTable());
                node.getAssignments().forEach(value -> action.accept(value));
                node.getWhere().ifPresent(where -> action.accept(where));
            }
            case UpdateAssignment node -> {
                action.accept(node.getName());
                action.accept(node.getValue());
            }
            case ValueColumn node -> {
                node.getEmptyDefault().ifPresent(expression -> action.accept(expression));
                node.getErrorDefault().ifPresent(expression -> action.accept(expression));
            }
            case Values node -> {
                for (Expression row : node.getRows()) {
                    action.accept(row);
                }
            }
            case VariableDefinition node -> {
                action.accept(node.getExpression());
            }
            case WindowDefinition node -> {
                action.accept(node.getWindow());
            }
            case WindowFrame node -> {
                action.accept(node.getStart());
                if (node.getEnd().isPresent()) {
                    action.accept(node.getEnd().get());
                }
                for (MeasureDefinition measureDefinition : node.getMeasures()) {
                    action.accept(measureDefinition);
                }
                for (VariableDefinition variableDefinition : node.getVariableDefinitions()) {
                    action.accept(variableDefinition);
                }
            }
            case WindowOperation node -> {
                action.accept(node.getName());
                action.accept((Node) node.getWindow());
            }
            case WindowReference node -> {
                action.accept(node.getName());
            }
            case WindowSpecification node -> {
                if (node.getExistingWindowName().isPresent()) {
                    action.accept(node.getExistingWindowName().get());
                }

                for (Expression expression : node.getPartitionBy()) {
                    action.accept(expression);
                }

                if (node.getOrderBy().isPresent()) {
                    action.accept(node.getOrderBy().get());
                }

                if (node.getFrame().isPresent()) {
                    action.accept(node.getFrame().get());
                }
            }
            case With node -> {
                for (WithQuery query : node.getQueries()) {
                    action.accept(query);
                }
            }
            case WithQuery node -> {
                action.accept(node.getQuery());
            }
            default -> {}
        }
    }
}
