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

package io.trino.sql.planner.planprinter.anonymize;

import com.google.common.base.Joiner;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.planprinter.TypedSymbol;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.ExpressionUtils.unResolveFunctions;
import static io.trino.sql.planner.Partitioning.ArgumentBinding;
import static io.trino.sql.planner.plan.AggregationNode.Aggregation;
import static io.trino.sql.planner.plan.WindowNode.Function;
import static io.trino.sql.planner.planprinter.anonymize.AggregationNodeRepresentation.AggregationRepresentation;
import static io.trino.sql.planner.planprinter.anonymize.TableScanNodeRepresentation.DomainRepresentation;
import static java.lang.String.format;

public final class AnonymizationUtils
{
    private AnonymizationUtils() {}

    public enum ObjectType
    {
        CATALOG,
        SCHEMA,
        TABLE,
        COLUMN,
        SYMBOL,
        LITERAL
    }

    public static List<TypedSymbol> anonymize(List<Symbol> outputSymbols, TypeProvider typeProvider)
    {
        return outputSymbols.stream()
                .map(symbol -> new TypedSymbol(anonymize(symbol), typeProvider.get(symbol).getDisplayName()))
                .collect(toImmutableList());
    }

    public static AggregationRepresentation anonymize(Aggregation aggregation)
    {
        return new AggregationRepresentation(
                aggregation.getResolvedFunction().getSignature().getName(),
                anonymizeAndUnResolveExpressions(aggregation.getArguments()),
                aggregation.isDistinct(),
                aggregation.getFilter().map(AnonymizationUtils::anonymize),
                aggregation.getOrderingScheme().map(AnonymizationUtils::anonymize),
                aggregation.getMask().map(AnonymizationUtils::anonymize));
    }

    public static String anonymize(Function function)
    {
        return format(
                "%s (%s)",
                function.getResolvedFunction().getSignature().getName(),
                Joiner.on(", ").join(anonymizeAndUnResolveExpressions(function.getArguments())));
    }

    public static Map<Symbol, SortOrder> anonymize(OrderingScheme orderingScheme)
    {
        return orderingScheme.getOrderings().entrySet().stream()
                        .collect(toImmutableMap(entry -> anonymize(entry.getKey()), Map.Entry::getValue));
    }

    public static ArgumentBinding anonymize(ArgumentBinding argument)
    {
        return new ArgumentBinding(anonymize(unResolveFunctions(argument.getExpression())), argument.getConstant());
    }

    public static List<Symbol> anonymize(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(AnonymizationUtils::anonymize)
                .collect(toImmutableList());
    }

    public static Symbol anonymize(Symbol symbol)
    {
        return new Symbol(anonymize(symbol.getName(), ObjectType.SYMBOL));
    }

    public static List<String> anonymizeAndUnResolveExpressions(List<Expression> expressions)
    {
        return expressions.stream()
                .map(ExpressionUtils::unResolveFunctions)
                .map(AnonymizationUtils::anonymize)
                .map(Expression::toString)
                .collect(toImmutableList());
    }

    public static Expression anonymize(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(
                new ExpressionRewriter<>()
                {
                    @Override
                    public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        return anonymize(Symbol.from(node)).toSymbolReference();
                    }

                    @Override
                    public Expression rewriteLiteral(Literal node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        if (node instanceof StringLiteral) {
                            return new StringLiteral(anonymize(((StringLiteral) node).getValue(), ObjectType.LITERAL));
                        }
                        else if (node instanceof GenericLiteral) {
                            return new GenericLiteral(
                                    ((GenericLiteral) node).getType(),
                                    anonymize(((GenericLiteral) node).getValue(), ObjectType.LITERAL));
                        }
                        else if (node instanceof CharLiteral) {
                            return new CharLiteral(anonymize(((CharLiteral) node).getValue(), ObjectType.LITERAL));
                        }
                        else if (node instanceof BinaryLiteral) {
                            return new BinaryLiteral(anonymize(((BinaryLiteral) node).getValue(), ObjectType.LITERAL));
                        }
                        return node;
                    }
                },
                expression);
    }

    public static Optional<Map<String, DomainRepresentation>> anonymize(TupleDomain<ColumnHandle> tupleDomain)
    {
        return tupleDomain.getDomains()
                .map(domains -> domains.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> anonymize(entry.getKey(), ObjectType.COLUMN),
                                entry -> anonymize(entry.getValue()))));
    }

    public static DomainRepresentation anonymize(Domain domain)
    {
        ValueSet values = domain.getValues();
        Optional<Integer> valueSetCount = Optional.empty();
        if (values instanceof EquatableValueSet) {
            valueSetCount = Optional.of(((EquatableValueSet) values).getValuesCount());
        }
        else if (values instanceof SortedRangeSet) {
            valueSetCount = Optional.of(((SortedRangeSet) values).getRangeCount());
        }
        return new DomainRepresentation(
                domain.getValues().getClass().getSimpleName(),
                domain.getType().getDisplayName(),
                valueSetCount,
                domain.isNullAllowed(),
                domain.isNone(),
                domain.isAll(),
                domain.isSingleValue());
    }

    public static <T> String anonymize(T object, ObjectType objectType)
    {
        return objectType.name().toLowerCase(Locale.US) + "_" + object.hashCode();
    }
}
