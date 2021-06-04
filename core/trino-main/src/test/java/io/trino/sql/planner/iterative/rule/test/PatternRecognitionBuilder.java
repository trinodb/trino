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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SymbolReference;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;

public class PatternRecognitionBuilder
{
    private PlanNode source;
    private List<Symbol> partitionBy = ImmutableList.of();
    private Optional<OrderingScheme> orderBy = Optional.empty();
    private final Map<Symbol, WindowNode.Function> windowFunctions = new HashMap<>();
    private final Map<Symbol, Map.Entry<String, Type>> measures = new HashMap<>();
    private Optional<WindowNode.Frame> commonBaseFrame = Optional.empty();
    private RowsPerMatch rowsPerMatch = ONE;
    private Optional<IrLabel> skipToLabel = Optional.empty();
    private SkipTo.Position skipToPosition = PAST_LAST;
    private boolean initial = true;
    private IrRowPattern pattern;
    private final Map<IrLabel, Set<IrLabel>> subsets = new HashMap<>();
    private final Map<IrLabel, String> variableDefinitions = new HashMap<>();

    public PatternRecognitionBuilder source(PlanNode source)
    {
        this.source = source;
        return this;
    }

    public PatternRecognitionBuilder partitionBy(List<Symbol> partitionBy)
    {
        this.partitionBy = partitionBy;
        return this;
    }

    public PatternRecognitionBuilder orderBy(OrderingScheme orderingScheme)
    {
        this.orderBy = Optional.of(orderingScheme);
        return this;
    }

    public PatternRecognitionBuilder addWindowFunction(Symbol symbol, WindowNode.Function function)
    {
        this.windowFunctions.put(symbol, function);
        return this;
    }

    public PatternRecognitionBuilder addMeasure(Symbol symbol, String expression, Type type)
    {
        this.measures.put(symbol, new AbstractMap.SimpleEntry<>(expression, type));
        return this;
    }

    public PatternRecognitionBuilder frame(WindowNode.Frame frame)
    {
        this.commonBaseFrame = Optional.of(frame);
        return this;
    }

    public PatternRecognitionBuilder rowsPerMatch(RowsPerMatch rowsPerMatch)
    {
        this.rowsPerMatch = rowsPerMatch;
        return this;
    }

    public PatternRecognitionBuilder skipTo(SkipTo.Position position, IrLabel label)
    {
        this.skipToPosition = position;
        this.skipToLabel = Optional.of(label);
        return this;
    }

    public PatternRecognitionBuilder skipTo(SkipTo.Position position)
    {
        this.skipToPosition = position;
        return this;
    }

    public PatternRecognitionBuilder seek()
    {
        this.initial = false;
        return this;
    }

    public PatternRecognitionBuilder pattern(IrRowPattern pattern)
    {
        this.pattern = pattern;
        return this;
    }

    public PatternRecognitionBuilder addSubset(IrLabel name, Set<IrLabel> elements)
    {
        this.subsets.put(name, elements);
        return this;
    }

    public PatternRecognitionBuilder addVariableDefinition(IrLabel name, String expression)
    {
        this.variableDefinitions.put(name, expression);
        return this;
    }

    public PatternRecognitionNode build(PlanNodeIdAllocator idAllocator)
    {
        return new PatternRecognitionNode(
                idAllocator.getNextId(),
                source,
                new WindowNode.Specification(partitionBy, orderBy),
                Optional.empty(),
                ImmutableSet.of(),
                0,
                windowFunctions,
                measures.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> measure(entry.getValue()))),
                commonBaseFrame,
                rowsPerMatch,
                skipToLabel,
                skipToPosition,
                initial,
                pattern,
                subsets,
                variableDefinitions.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> rewrite(entry.getValue()))));
    }

    private Measure measure(Map.Entry<String, Type> entry)
    {
        return new Measure(rewrite(entry.getKey()), entry.getValue());
    }

    private ExpressionAndValuePointers rewrite(String sql)
    {
        Expression expression = rewriteIdentifiers(new SqlParser().createExpression(sql, new ParsingOptions()));
        Map<Symbol, Type> types = extractExpressions(ImmutableList.of(expression), SymbolReference.class).stream()
                .collect(toImmutableMap(Symbol::from, reference -> BIGINT));
        return LogicalIndexExtractor.rewrite(expression, subsets, new SymbolAllocator(types));
    }

    public static Expression rewriteIdentifiers(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkArgument(node.getBase() instanceof Identifier, "chained dereferences not supported");
                return new LabelDereference(((Identifier) node.getBase()).getCanonicalValue(), new SymbolReference(node.getField().getValue()));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }

            @Override
            public Expression rewriteGenericDataType(GenericDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers within type parameters
                return node;
            }

            @Override
            public Expression rewriteRowDataType(RowDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers in field names
                return node;
            }
        }, expression);
    }
}
