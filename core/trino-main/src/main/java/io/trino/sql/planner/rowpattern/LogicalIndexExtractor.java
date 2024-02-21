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
package io.trino.sql.planner.rowpattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.isPatternRecognitionFunction;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Rewriter for expressions specific to row pattern recognition.
 * Removes label-prefixed symbol references from the expression and replaces them with symbols.
 * Removes row pattern navigation functions (PREV, NEXT, FIRST and LAST) from the expression.
 * Removes pattern special functions CLASSIFIER() and MATCH_NUMBER() and replaces them with symbols.
 * Reallocates all symbols in the expression to avoid unwanted optimizations when the expression is compiled.
 * For each of the symbols creates a value accessor (ValuePointer).
 * Returns new symbols as expected "input descriptor", upon which the rewritten expression will be compiled, along with value accessors.
 * Value accessors are ordered the same way as the corresponding symbols so that they can be used to provide actual values to the compiled expression.
 * Each time the compiled expression will be executed, a single-row input will be prepared with the use of the value accessors,
 * following the symbols layout.
 * <p>
 * Aggregate functions in pattern recognition expressions are handled in special way. Similarly to column references, CLASSIFIER() and MATCH_NUMBER()
 * calls, they are replaced with a single symbol in the resulting expression, backed with a ValuePointer. The ValuePointer is an instance of the
 * AggregationValuePointer class. It captures the aggregate function, the descriptor of the aggregated set of rows, and a list of arguments.
 * The expressions in the arguments list are rewritten so that they do not contain any pattern-recognition-specific elements.
 */
public class LogicalIndexExtractor
{
    public static ExpressionAndValuePointers rewrite(Expression expression, Map<IrLabel, Set<IrLabel>> subsets, SymbolAllocator symbolAllocator, Metadata metadata)
    {
        ImmutableList.Builder<ExpressionAndValuePointers.Assignment> assignments = ImmutableList.builder();

        Visitor visitor = new Visitor(subsets, assignments, symbolAllocator, metadata);
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(visitor, expression, LogicalIndexContext.DEFAULT);

        return new ExpressionAndValuePointers(rewritten, assignments.build());
    }

    private LogicalIndexExtractor() {}

    private static class Visitor
            extends ExpressionRewriter<LogicalIndexContext>
    {
        private final Map<IrLabel, Set<IrLabel>> subsets;
        private final ImmutableList.Builder<ExpressionAndValuePointers.Assignment> assignments;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;

        public Visitor(
                Map<IrLabel, Set<IrLabel>> subsets,
                ImmutableList.Builder<ExpressionAndValuePointers.Assignment> assignments,
                SymbolAllocator symbolAllocator,
                Metadata metadata)
        {
            this.subsets = requireNonNull(subsets, "subsets is null");
            this.assignments = requireNonNull(assignments, "assignments is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        protected Expression rewriteExpression(Expression node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteLabelDereference(LabelDereference node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            Symbol referenced = Symbol.from(node.getReference().orElseThrow());
            Symbol reallocated = symbolAllocator.newSymbol(referenced);
            Set<IrLabel> labels = subsets.get(irLabel(node.getLabel()));
            if (labels == null) {
                labels = ImmutableSet.of(irLabel(node.getLabel()));
            }
            assignments.add(new ExpressionAndValuePointers.Assignment(reallocated, new ScalarValuePointer(context.withLabels(labels).toLogicalIndexPointer(), referenced)));
            return reallocated.toSymbolReference();
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            // symbol reference with no label prefix is implicitly prefixed with a universal row pattern variable (matches every label)
            // it is encoded as empty label set
            Symbol reallocated = symbolAllocator.newSymbol(Symbol.from(node));
            assignments.add(new ExpressionAndValuePointers.Assignment(reallocated, new ScalarValuePointer(context.withLabels(ImmutableSet.of()).toLogicalIndexPointer(), Symbol.from(node))));
            return reallocated.toSymbolReference();
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            if (isPatternRecognitionFunction(node)) {
                QualifiedName name = node.getName();
                String functionName = name.getSuffix().toUpperCase(ENGLISH);
                return switch (functionName) {
                    case "FIRST", "LAST", "PREV", "NEXT" -> rewritePatternNavigationFunction(node, context, treeRewriter);
                    case "CLASSIFIER" -> rewriteClassifierFunction(node, context);
                    case "MATCH_NUMBER" -> rewriteMatchNumberFunction();
                    default -> throw new UnsupportedOperationException("unsupported pattern recognition function type: " + node.getName());
                };
            }

            ResolvedFunction resolvedFunction = metadata.decodeFunction(node.getName());
            if (resolvedFunction.getFunctionKind() == FunctionKind.AGGREGATE) {
                Type type = resolvedFunction.getSignature().getReturnType();

                Symbol aggregationSymbol = symbolAllocator.newSymbol(node, type);

                Symbol classifierSymbol = symbolAllocator.newSymbol("classifier", VARCHAR);
                Symbol matchNumberSymbol = symbolAllocator.newSymbol("match_number", BIGINT);
                List<Expression> rewrittenArguments = AggregateArgumentsRewriter.rewrite(node.getArguments(), classifierSymbol, matchNumberSymbol);

                AggregationValuePointer descriptor = new AggregationValuePointer(
                        resolvedFunction,
                        new AggregatedSetDescriptor(
                                extractLabels(node),
                                node.getProcessingMode().isEmpty() || node.getProcessingMode().get().getMode() != FINAL),
                        rewrittenArguments,
                        classifierSymbol,
                        matchNumberSymbol);

                assignments.add(new ExpressionAndValuePointers.Assignment(aggregationSymbol, descriptor));

                return aggregationSymbol.toSymbolReference();
            }

            return super.rewriteFunctionCall(node, context, treeRewriter);
        }

        /**
         * Extract labels to identify rows to which the aggregation should be applied.
         * It is assumed that all arguments of the aggregation apply consistently to the same set of labels.
         *
         * @param node a `FunctionCall` for the aggregate function
         * @return set of `IrLabel`s corresponding to primary row pattern variables or empty set for the universal row pattern variable
         */
        private Set<IrLabel> extractLabels(FunctionCall node)
        {
            if (node.getArguments().isEmpty()) {
                return ImmutableSet.of();
            }

            List<LabelDereference> labeledDereferences = extractExpressions(node.getArguments(), LabelDereference.class);
            if (!labeledDereferences.isEmpty()) {
                IrLabel label = irLabel(labeledDereferences.get(0).getLabel());
                Set<IrLabel> labels = subsets.get(label);
                if (labels == null) {
                    labels = ImmutableSet.of(label);
                }
                return labels;
            }

            Optional<FunctionCall> classifierCall = extractExpressions(node.getArguments(), FunctionCall.class).stream()
                    .filter(ExpressionAnalyzer::isPatternRecognitionFunction)
                    .filter(function -> function.getName().getSuffix().toUpperCase(ENGLISH).equals("CLASSIFIER"))
                    .findFirst();

            if (classifierCall.isPresent()) {
                FunctionCall classifier = classifierCall.get();
                if (!classifier.getArguments().isEmpty()) {
                    IrLabel label = irLabel(((Identifier) getOnlyElement(classifier.getArguments())).getCanonicalValue());
                    Set<IrLabel> labels = subsets.get(label);
                    if (labels == null) {
                        labels = ImmutableSet.of(label);
                    }
                    return labels;
                }
            }

            return ImmutableSet.of();
        }

        private Expression rewritePatternNavigationFunction(FunctionCall node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            String functionName = node.getName().getSuffix().toUpperCase(ENGLISH);
            Expression argument = node.getArguments().get(0);
            Optional<ProcessingMode> processingMode = node.getProcessingMode();
            OptionalInt offset = OptionalInt.empty();
            if (node.getArguments().size() > 1) {
                offset = OptionalInt.of(toIntExact(((LongLiteral) node.getArguments().get(1)).getParsedValue()));
            }
            return switch (functionName) {
                case "PREV" -> treeRewriter.rewrite(argument, context.withPhysicalOffset(-offset.orElse(1)));
                case "NEXT" -> treeRewriter.rewrite(argument, context.withPhysicalOffset(offset.orElse(1)));
                case "FIRST" -> treeRewriter.rewrite(argument, context.withLogicalOffset(
                        processingMode.isEmpty() || processingMode.get().getMode() != FINAL,
                        false,
                        offset.orElse(0)));
                case "LAST" -> treeRewriter.rewrite(argument, context.withLogicalOffset(
                        processingMode.isEmpty() || processingMode.get().getMode() != FINAL,
                        true,
                        offset.orElse(0)));
                default -> throw new UnsupportedOperationException("unsupported pattern navigation function type: " + node.getName());
            };
        }

        private Expression rewriteClassifierFunction(FunctionCall node, LogicalIndexContext context)
        {
            Symbol classifierSymbol = symbolAllocator.newSymbol("classifier", VARCHAR);

            Set<IrLabel> labels = ImmutableSet.of();
            if (!node.getArguments().isEmpty()) {
                IrLabel label = irLabel(((Identifier) getOnlyElement(node.getArguments())).getCanonicalValue());
                labels = subsets.get(label);
                if (labels == null) {
                    labels = ImmutableSet.of(label);
                }
            }

            // pass the new symbol as input symbol. It will be used to identify classifier function.
            assignments.add(new ExpressionAndValuePointers.Assignment(classifierSymbol, new ClassifierValuePointer(context.withLabels(labels).toLogicalIndexPointer())));
            return classifierSymbol.toSymbolReference();
        }

        private Expression rewriteMatchNumberFunction()
        {
            Symbol matchNumberSymbol = symbolAllocator.newSymbol("match_number", BIGINT);
            // pass default LogicalIndexPointer. It will not be accessed. match_number() is constant in the context of a match.
            // pass the new symbol as input symbol. It will be used to identify match number function.
            assignments.add(new ExpressionAndValuePointers.Assignment(matchNumberSymbol, new MatchNumberValuePointer()));
            return matchNumberSymbol.toSymbolReference();
        }

        private IrLabel irLabel(String label)
        {
            return new IrLabel(label);
        }
    }

    private static class LogicalIndexContext
    {
        public static final LogicalIndexContext DEFAULT = new LogicalIndexContext(ImmutableSet.of(), true, true, 0, 0);

        private final Set<IrLabel> label;
        private final boolean running;
        private final boolean last;
        private final int logicalOffset;
        private final int physicalOffset;

        private LogicalIndexContext(Set<IrLabel> label, boolean running, boolean last, int logicalOffset, int physicalOffset)
        {
            this.label = requireNonNull(label, "label is null");
            this.running = running;
            this.last = last;
            this.logicalOffset = logicalOffset;
            this.physicalOffset = physicalOffset;
        }

        public LogicalIndexContext withPhysicalOffset(int physicalOffset)
        {
            return new LogicalIndexContext(this.label, this.running, this.last, this.logicalOffset, physicalOffset);
        }

        public LogicalIndexContext withLogicalOffset(boolean running, boolean last, int logicalOffset)
        {
            return new LogicalIndexContext(this.label, running, last, logicalOffset, this.physicalOffset);
        }

        public LogicalIndexContext withLabels(Set<IrLabel> labels)
        {
            return new LogicalIndexContext(labels, this.running, this.last, this.logicalOffset, this.physicalOffset);
        }

        public LogicalIndexPointer toLogicalIndexPointer()
        {
            return new LogicalIndexPointer(label, last, running, logicalOffset, physicalOffset);
        }
    }
}
