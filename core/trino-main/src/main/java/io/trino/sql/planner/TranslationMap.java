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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ExpressionAnalyzer.LabelPrefixedReference;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Trim;
import io.trino.sql.util.AstUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Keeps mappings of fields and AST expressions to symbols in the current plan within query boundary.
 * <p>
 * AST and IR expressions use the same class hierarchy ({@link io.trino.sql.tree.Expression},
 * but differ in the following ways:
 * <li>AST expressions contain Identifiers, while IR expressions contain SymbolReferences</li>
 * <li>FunctionCalls in AST expressions are SQL function names. In IR expressions, they contain an encoded name representing a resolved function</li>
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final Scope scope;
    private final Analysis analysis;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments;
    private final Optional<TranslationMap> outerContext;

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<ScopeAware<Expression>, Symbol> astToSymbols;

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]).clone(), ImmutableMap.of());
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]), astToSymbols);
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, Symbol[] fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols)
    {
        this.outerContext = requireNonNull(outerContext, "outerContext is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaArguments = requireNonNull(lambdaArguments, "lambdaArguments is null");

        requireNonNull(fieldSymbols, "fieldSymbols is null");
        this.fieldSymbols = fieldSymbols.clone();

        requireNonNull(astToSymbols, "astToSymbols is null");
        this.astToSymbols = ImmutableMap.copyOf(astToSymbols);

        checkArgument(scope.getLocalScopeFieldCount() == fieldSymbols.length,
                "scope: %s, fields mappings: %s",
                scope.getRelationType().getAllFieldCount(),
                fieldSymbols.length);

        astToSymbols.keySet().stream()
                .map(ScopeAware::getNode)
                .forEach(TranslationMap::verifyAstExpression);
    }

    public TranslationMap withScope(Scope scope, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields.toArray(new Symbol[0]), astToSymbols);
    }

    public TranslationMap withNewMappings(Map<ScopeAware<Expression>, Symbol> mappings, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields, mappings);
    }

    public TranslationMap withAdditionalMappings(Map<ScopeAware<Expression>, Symbol> mappings)
    {
        Map<ScopeAware<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.astToSymbols);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, newMappings);
    }

    public List<Symbol> getFieldSymbols()
    {
        return Collections.unmodifiableList(Arrays.asList(fieldSymbols));
    }

    public Map<ScopeAware<Expression>, Symbol> getMappings()
    {
        return astToSymbols;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public boolean canTranslate(Expression expression)
    {
        verifyAstExpression(expression);

        if (astToSymbols.containsKey(scopeAwareKey(expression, analysis, scope)) || expression instanceof FieldReference) {
            return true;
        }

        if (analysis.isColumnReference(expression)) {
            ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));
            return scope.isLocalScope(field.getScope());
        }

        return false;
    }

    public Expression rewrite(Expression expression)
    {
        verifyAstExpression(expression);

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }

            @Override
            public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                return getSymbolForColumn(node)
                        .map(Symbol::toSymbolReference)
                        .orElseThrow(() -> new IllegalStateException(format("No symbol mapping for node '%s' (%s)", node, node.getFieldIndex())));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(node);
                if (referencedLambdaArgumentDeclaration != null) {
                    Symbol symbol = lambdaArguments.get(NodeRef.of(referencedLambdaArgumentDeclaration));
                    return coerceIfNecessary(node, symbol.toSymbolReference());
                }

                return getSymbolForColumn(node)
                        .map(symbol -> coerceIfNecessary(node, symbol.toSymbolReference()))
                        .orElse(coerceIfNecessary(node, node));
            }

            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isPatternRecognitionFunction(node)) {
                    ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();
                    if (!node.getArguments().isEmpty()) {
                        rewrittenArguments.add(treeRewriter.rewrite(node.getArguments().get(0), null));
                        if (node.getArguments().size() > 1) {
                            // do not rewrite the offset literal
                            rewrittenArguments.add(node.getArguments().get(1));
                        }
                    }
                    // Pattern recognition functions are special constructs, passed using the form of FunctionCall.
                    // They are not resolved like regular function calls. They are processed in LogicalIndexExtractor.
                    return coerceIfNecessary(node, new FunctionCall(
                            Optional.empty(),
                            node.getName(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty(),
                            node.getProcessingMode(),
                            rewrittenArguments.build()));
                }

                // Do not use the mapping for aggregate functions in pattern recognition context. They have different semantics
                // than aggregate functions outside pattern recognition.
                if (!analysis.isPatternAggregation(node)) {
                    Optional<Expression> mapped = tryGetMapping(node);
                    if (mapped.isPresent()) {
                        return coerceIfNecessary(node, mapped.get());
                    }
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
                rewritten = new FunctionCall(
                        rewritten.getLocation(),
                        resolvedFunction.toQualifiedName(),
                        rewritten.getWindow(),
                        rewritten.getFilter(),
                        rewritten.getOrderBy(),
                        rewritten.isDistinct(),
                        rewritten.getNullTreatment(),
                        rewritten.getProcessingMode(),
                        rewritten.getArguments());
                return coerceIfNecessary(node, rewritten);
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                LabelPrefixedReference labelDereference = analysis.getLabelDereference(node);
                if (labelDereference != null) {
                    if (labelDereference.getColumn().isPresent()) {
                        Expression rewritten = treeRewriter.rewrite(labelDereference.getColumn().get(), null);
                        checkState(rewritten instanceof SymbolReference, "expected symbol reference, got: " + rewritten);
                        return coerceIfNecessary(node, new LabelDereference(labelDereference.getLabel(), (SymbolReference) rewritten));
                    }
                    return new LabelDereference(labelDereference.getLabel());
                }

                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                if (analysis.isColumnReference(node)) {
                    return coerceIfNecessary(
                            node,
                            getSymbolForColumn(node)
                                    .map(Symbol::toSymbolReference)
                                    .orElseThrow(() -> new IllegalStateException(format("No mapping for %s", node))));
                }

                RowType rowType = (RowType) analysis.getType(node.getBase());
                String fieldName = node.getField().orElseThrow().getValue();

                List<RowType.Field> fields = rowType.getFields();
                int index = -1;
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                        checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                        index = i;
                    }
                }

                checkState(index >= 0, "could not find field name: %s", fieldName);

                return coerceIfNecessary(
                        node,
                        new SubscriptExpression(
                                treeRewriter.rewrite(node.getBase(), context),
                                new LongLiteral(Long.toString(index + 1))));
            }

            @Override
            public Expression rewriteTrim(Trim node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                Trim rewritten = treeRewriter.defaultRewrite(node, context);

                ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
                arguments.add(rewritten.getTrimSource());
                rewritten.getTrimCharacter().ifPresent(arguments::add);

                FunctionCall functionCall = new FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());
                return coerceIfNecessary(node, functionCall);
            }

            @Override
            public Expression rewriteSubscriptExpression(SubscriptExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Type baseType = analysis.getType(node.getBase());
                if (baseType instanceof RowType) {
                    // Do not rewrite subscript index into symbol. Row subscript index is required to be a literal.
                    Expression rewrittenBase = treeRewriter.rewrite(node.getBase(), context);
                    return coerceIfNecessary(node, new SubscriptExpression(rewrittenBase, node.getIndex()));
                }

                Expression rewritten = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewritten);
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");

                ImmutableList.Builder<LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
                for (LambdaArgumentDeclaration argument : node.getArguments()) {
                    Symbol symbol = lambdaArguments.get(NodeRef.of(argument));
                    newArguments.add(new LambdaArgumentDeclaration(new Identifier(symbol.getName())));
                }
                Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), null);
                return new LambdaExpression(newArguments.build(), rewrittenBody);
            }

            @Override
            public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                checkState(analysis.getParameters().size() > node.getPosition(), "Too few parameter values");
                return coerceIfNecessary(node, treeRewriter.rewrite(analysis.getParameters().get(NodeRef.of(node)), null));
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

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
                // Don't add a coercion for the top-level expression. That depends on the context the expression is used and it's the responsibility of the caller.
                if (original == expression) {
                    return rewritten;
                }

                return QueryPlanner.coerceIfNecessary(analysis, original, rewritten);
            }
        }, expression, null);
    }

    private Optional<Expression> tryGetMapping(Expression expression)
    {
        return Optional.ofNullable(astToSymbols.get(scopeAwareKey(expression, analysis, scope)))
                .map(Symbol::toSymbolReference);
    }

    private Optional<Symbol> getSymbolForColumn(Expression expression)
    {
        if (!analysis.isColumnReference(expression)) {
            // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda argument reference).
            // In such case, the expression might still be resolvable with plan.getScope() but we should not resolve it.
            return Optional.empty();
        }

        ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));

        if (scope.isLocalScope(field.getScope())) {
            return Optional.of(fieldSymbols[field.getHierarchyFieldIndex()]);
        }

        if (outerContext.isPresent()) {
            return Optional.of(Symbol.from(outerContext.get().rewrite(expression)));
        }

        return Optional.empty();
    }

    private static void verifyAstExpression(Expression astExpression)
    {
        verify(AstUtils.preOrder(astExpression).noneMatch(expression ->
                expression instanceof SymbolReference ||
                        expression instanceof FunctionCall && ResolvedFunction.isResolved(((FunctionCall) expression).getName())));
    }

    public Scope getScope()
    {
        return scope;
    }
}
