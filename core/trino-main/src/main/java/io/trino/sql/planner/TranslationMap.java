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
import io.trino.Session;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ExpressionAnalyzer.LabelPrefixedReference;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.ir.AstToIrExpressionRewriter;
import io.trino.sql.ir.AstToIrExpressionTreeRewriter;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Trim;
import io.trino.sql.util.AstUtils;
import io.trino.type.JsonPath2016Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.KEEP;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.OMIT;
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
public class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final Scope scope;
    private final Analysis analysis;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments;
    private final Optional<TranslationMap> outerContext;
    private final Session session;
    private final PlannerContext plannerContext;

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<ScopeAware<Expression>, Symbol> astToSymbols;

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Session session, PlannerContext plannerContext)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]).clone(), ImmutableMap.of(), session, plannerContext);
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, List<Symbol> fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols, Session session, PlannerContext plannerContext)
    {
        this(outerContext, scope, analysis, lambdaArguments, fieldSymbols.toArray(new Symbol[0]), astToSymbols, session, plannerContext);
    }

    public TranslationMap(Optional<TranslationMap> outerContext, Scope scope, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, Symbol[] fieldSymbols, Map<ScopeAware<Expression>, Symbol> astToSymbols, Session session, PlannerContext plannerContext)
    {
        this.outerContext = requireNonNull(outerContext, "outerContext is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaArguments = requireNonNull(lambdaArguments, "lambdaArguments is null");
        this.session = requireNonNull(session, "session is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");

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
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields.toArray(new Symbol[0]), astToSymbols, session, plannerContext);
    }

    public TranslationMap withNewMappings(Map<ScopeAware<Expression>, Symbol> mappings, List<Symbol> fields)
    {
        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fields, mappings, session, plannerContext);
    }

    public TranslationMap withAdditionalMappings(Map<ScopeAware<Expression>, Symbol> mappings)
    {
        Map<ScopeAware<Expression>, Symbol> newMappings = new HashMap<>();
        newMappings.putAll(this.astToSymbols);
        newMappings.putAll(mappings);

        return new TranslationMap(outerContext, scope, analysis, lambdaArguments, fieldSymbols, newMappings, session, plannerContext);
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

    public io.trino.sql.ir.Expression rewriteAstExpressionToIrExpression(Expression expression)
    {
        verifyAstExpression(expression);

        return AstToIrExpressionTreeRewriter.rewriteWith(new AstToIrExpressionRewriter<Void>()
        {
            @Override
            protected io.trino.sql.ir.Expression rewriteExpression(Expression node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                io.trino.sql.ir.Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteFieldReference(FieldReference node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                return getSymbolForColumn(node)
                        .map(Symbol::toIrSymbolReference)
                        .orElseThrow(() -> new IllegalStateException(format("No symbol mapping for node '%s' (%s)", node, node.getFieldIndex())));
            }

            @Override
            public io.trino.sql.ir.Expression rewriteIdentifier(Identifier node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(node);
                if (referencedLambdaArgumentDeclaration != null) {
                    Symbol symbol = lambdaArguments.get(NodeRef.of(referencedLambdaArgumentDeclaration));
                    return coerceIfNecessary(node, symbol.toIrSymbolReference(), treeRewriter);
                }

                return getSymbolForColumn(node)
                        .map(symbol -> coerceIfNecessary(node, symbol.toIrSymbolReference(), treeRewriter))
                        .orElse(coerceIfNecessary(node, treeRewriter.copy(node, context), treeRewriter));
            }

            @Override
            public io.trino.sql.ir.Expression rewriteFunctionCall(FunctionCall node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isPatternRecognitionFunction(node)) {
                    ImmutableList.Builder<io.trino.sql.ir.Expression> rewrittenArguments = ImmutableList.builder();
                    if (!node.getArguments().isEmpty()) {
                        rewrittenArguments.add(treeRewriter.rewrite(node.getArguments().get(0), null));
                        if (node.getArguments().size() > 1) {
                            // do not rewrite the offset literal
                            rewrittenArguments.add(treeRewriter.copy(node.getArguments().get(1), null));
                        }
                    }
                    // Pattern recognition functions are special constructs, passed using the form of FunctionCall.
                    // They are not resolved like regular function calls. They are processed in LogicalIndexExtractor.
                    return coerceIfNecessary(node, new io.trino.sql.ir.FunctionCall(
                            convertQualifiedName(node.getName()),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty(),
                            treeRewriter.copy(node.getProcessingMode(), null),
                            rewrittenArguments.build()), treeRewriter);
                }

                // Do not use the mapping for aggregate functions in pattern recognition context. They have different semantics
                // than aggregate functions outside pattern recognition.
                if (!analysis.isPatternAggregation(node)) {
                    Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), null);
                    if (mapped.isPresent()) {
                        return coerceIfNecessary(node, mapped.get(), treeRewriter);
                    }
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                io.trino.sql.ir.FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
                rewritten = new io.trino.sql.ir.FunctionCall(
                        resolvedFunction.toQualifiedName(),
                        rewritten.getWindow(),
                        rewritten.getFilter(),
                        rewritten.getOrderBy(),
                        rewritten.isDistinct(),
                        rewritten.getNullTreatment(),
                        rewritten.getProcessingMode(),
                        rewritten.getArguments());
                return coerceIfNecessary(node, rewritten, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                LabelPrefixedReference labelDereference = analysis.getLabelDereference(node);
                if (labelDereference != null) {
                    if (labelDereference.getColumn().isPresent()) {
                        io.trino.sql.ir.Expression rewritten = treeRewriter.rewrite(labelDereference.getColumn().get(), null);
                        checkState(rewritten instanceof io.trino.sql.ir.SymbolReference, "expected symbol reference, got: " + rewritten);
                        return coerceIfNecessary(node, new io.trino.sql.ir.LabelDereference(labelDereference.getLabel(), (io.trino.sql.ir.SymbolReference) rewritten), treeRewriter);
                    }
                    return new io.trino.sql.ir.LabelDereference(labelDereference.getLabel());
                }

                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                if (analysis.isColumnReference(node)) {
                    return coerceIfNecessary(
                            node,
                            getSymbolForColumn(node)
                                    .map(Symbol::toIrSymbolReference)
                                    .orElseThrow(() -> new IllegalStateException(format("No mapping for %s", node))), treeRewriter);
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
                        new io.trino.sql.ir.SubscriptExpression(
                                treeRewriter.rewrite(node.getBase(), context),
                                new io.trino.sql.ir.LongLiteral(Long.toString(index + 1))), treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteTrim(Trim node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                io.trino.sql.ir.Trim rewritten = treeRewriter.defaultRewrite(node, context);

                ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.builder();
                arguments.add(rewritten.getTrimSource());
                rewritten.getTrimCharacter().ifPresent(arguments::add);

                io.trino.sql.ir.FunctionCall functionCall = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());
                return coerceIfNecessary(node, functionCall, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteSubscriptExpression(SubscriptExpression node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                Type baseType = analysis.getType(node.getBase());
                if (baseType instanceof RowType) {
                    // Do not rewrite subscript index into symbol. Row subscript index is required to be a literal.
                    io.trino.sql.ir.Expression rewrittenBase = treeRewriter.rewrite(node.getBase(), context);
                    return coerceIfNecessary(node, new io.trino.sql.ir.SubscriptExpression(rewrittenBase, treeRewriter.copy(node.getIndex(), context)), treeRewriter);
                }

                io.trino.sql.ir.Expression rewritten = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewritten, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteLambdaExpression(LambdaExpression node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");

                ImmutableList.Builder<io.trino.sql.ir.LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
                for (LambdaArgumentDeclaration argument : node.getArguments()) {
                    Symbol symbol = lambdaArguments.get(NodeRef.of(argument));
                    newArguments.add(new io.trino.sql.ir.LambdaArgumentDeclaration(new io.trino.sql.ir.Identifier(symbol.getName())));
                }
                io.trino.sql.ir.Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), null);
                return new io.trino.sql.ir.LambdaExpression(newArguments.build(), rewrittenBody);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteParameter(Parameter node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                checkState(analysis.getParameters().size() > node.getPosition(), "Too few parameter values");
                return coerceIfNecessary(node, treeRewriter.rewrite(analysis.getParameters().get(NodeRef.of(node)), null), treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteGenericDataType(GenericDataType node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers within type parameters
                return treeRewriter.copy(node, context);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteRowDataType(RowDataType node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers in field names
                return treeRewriter.copy(node, context);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteJsonExists(JsonExists node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                io.trino.sql.ir.JsonExists rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                io.trino.sql.ir.BooleanLiteral failOnError = new io.trino.sql.ir.BooleanLiteral(node.getErrorBehavior() == JsonExists.ErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                io.trino.sql.ir.Expression input = new io.trino.sql.ir.FunctionCall(inputToJson.toQualifiedName(),
                        ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                IrParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError,
                        treeRewriter);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                io.trino.sql.ir.Expression pathExpression = treeRewriter.copy(new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME))), context);

                ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));

                io.trino.sql.ir.Expression result = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                return coerceIfNecessary(node, result, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteJsonValue(JsonValue node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression, default expressions, and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                io.trino.sql.ir.JsonValue rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                io.trino.sql.ir.BooleanLiteral failOnError = new io.trino.sql.ir.BooleanLiteral(node.getErrorBehavior() == JsonValue.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                io.trino.sql.ir.Expression input = new io.trino.sql.ir.FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                IrParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError,
                        treeRewriter);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                io.trino.sql.ir.Expression pathExpression = treeRewriter.copy(new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME))), null);

                ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(rewritten.getEmptyDefault().orElse(treeRewriter.copy(new Cast(new NullLiteral(), toSqlType(resolvedFunction.getSignature().getReturnType())), context)))
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())))
                        .add(rewritten.getErrorDefault().orElse(treeRewriter.copy(new Cast(new NullLiteral(), toSqlType(resolvedFunction.getSignature().getReturnType())), context)));

                io.trino.sql.ir.Expression result = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                return coerceIfNecessary(node, result, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteJsonQuery(JsonQuery node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                io.trino.sql.ir.JsonQuery rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                io.trino.sql.ir.BooleanLiteral failOnError = new io.trino.sql.ir.BooleanLiteral(node.getErrorBehavior() == JsonQuery.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                io.trino.sql.ir.Expression input = new io.trino.sql.ir.FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                IrParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError,
                        treeRewriter);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                io.trino.sql.ir.Expression pathExpression = treeRewriter.copy(new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME))), context);

                ImmutableList.Builder<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getWrapperBehavior().ordinal())))
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));

                io.trino.sql.ir.Expression function = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                // apply function to format output
                io.trino.sql.ir.GenericLiteral errorBehavior = new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal()));
                io.trino.sql.ir.BooleanLiteral omitQuotes = new io.trino.sql.ir.BooleanLiteral(node.getQuotesBehavior().orElse(KEEP) == OMIT ? "true" : "false");
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                io.trino.sql.ir.Expression result = new io.trino.sql.ir.FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(function, errorBehavior, omitQuotes));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new io.trino.sql.ir.Cast(result, treeRewriter.copy(toSqlType(returnedType), context));
                }

                return coerceIfNecessary(node, result, treeRewriter);
            }

            private IrParametersRow getParametersRow(
                    List<JsonPathParameter> pathParameters,
                    List<io.trino.sql.ir.JsonPathParameter> rewrittenPathParameters,
                    Type parameterRowType,
                    io.trino.sql.ir.BooleanLiteral failOnError,
                    AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                io.trino.sql.ir.Expression parametersRow;
                List<String> parametersOrder;
                if (!pathParameters.isEmpty()) {
                    ImmutableList.Builder<io.trino.sql.ir.Expression> parameters = ImmutableList.builder();
                    for (int i = 0; i < pathParameters.size(); i++) {
                        ResolvedFunction parameterToJson = analysis.getJsonInputFunction(pathParameters.get(i).getParameter());
                        io.trino.sql.ir.Expression rewrittenParameter = rewrittenPathParameters.get(i).getParameter();
                        if (parameterToJson != null) {
                            parameters.add(new io.trino.sql.ir.FunctionCall(parameterToJson.toQualifiedName(), ImmutableList.of(rewrittenParameter, failOnError)));
                        }
                        else {
                            parameters.add(rewrittenParameter);
                        }
                    }
                    parametersRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.Row(parameters.build()), treeRewriter.copy(toSqlType(parameterRowType), null));
                    parametersOrder = pathParameters.stream()
                            .map(parameter -> parameter.getName().getCanonicalValue())
                            .collect(toImmutableList());
                }
                else {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(parameterRowType), "invalid type of parameters row when no parameters are passed");
                    parametersRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.NullLiteral(), treeRewriter.copy(toSqlType(JSON_NO_PARAMETERS_ROW_TYPE), null));
                    parametersOrder = ImmutableList.of();
                }

                return new IrParametersRow(parametersRow, parametersOrder);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteJsonObject(JsonObject node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                io.trino.sql.ir.Expression keysRow;
                io.trino.sql.ir.Expression valuesRow;

                // prepare keys and values as rows
                if (node.getMembers().isEmpty()) {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(0)));
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(1)));
                    keysRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.NullLiteral(), treeRewriter.copy(toSqlType(JSON_NO_PARAMETERS_ROW_TYPE), context));
                    valuesRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.NullLiteral(), treeRewriter.copy(toSqlType(JSON_NO_PARAMETERS_ROW_TYPE), context));
                }
                else {
                    ImmutableList.Builder<io.trino.sql.ir.Expression> keys = ImmutableList.builder();
                    ImmutableList.Builder<io.trino.sql.ir.Expression> values = ImmutableList.builder();
                    for (JsonObjectMember member : node.getMembers()) {
                        Expression key = member.getKey();
                        Expression value = member.getValue();

                        io.trino.sql.ir.Expression rewrittenKey = treeRewriter.rewrite(key, context);
                        keys.add(rewrittenKey);

                        io.trino.sql.ir.Expression rewrittenValue = treeRewriter.rewrite(value, context);
                        ResolvedFunction valueToJson = analysis.getJsonInputFunction(value);
                        if (valueToJson != null) {
                            values.add(new io.trino.sql.ir.FunctionCall(valueToJson.toQualifiedName(), ImmutableList.of(rewrittenValue, io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL)));
                        }
                        else {
                            values.add(rewrittenValue);
                        }
                    }
                    keysRow = new io.trino.sql.ir.Row(keys.build());
                    valuesRow = new io.trino.sql.ir.Row(values.build());
                }

                List<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(keysRow)
                        .add(valuesRow)
                        .add(node.isNullOnNull() ? io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL : io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL)
                        .add(node.isUniqueKeys() ? io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL : io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL)
                        .build();

                io.trino.sql.ir.Expression function = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments);

                // apply function to format output
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                io.trino.sql.ir.Expression result = new io.trino.sql.ir.FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(
                        function,
                        new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(JsonQuery.EmptyOrErrorBehavior.ERROR.ordinal())),
                        io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new io.trino.sql.ir.Cast(result, treeRewriter.copy(toSqlType(returnedType), context));
                }

                return coerceIfNecessary(node, result, treeRewriter);
            }

            @Override
            public io.trino.sql.ir.Expression rewriteJsonArray(JsonArray node, Void context, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<io.trino.sql.ir.Expression> mapped = treeRewriter.copy(tryGetMapping(node), context);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get(), treeRewriter);
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                io.trino.sql.ir.Expression elementsRow;

                // prepare elements as row
                if (node.getElements().isEmpty()) {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(0)));
                    elementsRow = new io.trino.sql.ir.Cast(new io.trino.sql.ir.NullLiteral(), treeRewriter.copy(toSqlType(JSON_NO_PARAMETERS_ROW_TYPE), context));
                }
                else {
                    ImmutableList.Builder<io.trino.sql.ir.Expression> elements = ImmutableList.builder();
                    for (JsonArrayElement arrayElement : node.getElements()) {
                        Expression element = arrayElement.getValue();
                        io.trino.sql.ir.Expression rewrittenElement = treeRewriter.rewrite(element, context);
                        ResolvedFunction elementToJson = analysis.getJsonInputFunction(element);
                        if (elementToJson != null) {
                            elements.add(new io.trino.sql.ir.FunctionCall(elementToJson.toQualifiedName(), ImmutableList.of(rewrittenElement, io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL)));
                        }
                        else {
                            elements.add(rewrittenElement);
                        }
                    }
                    elementsRow = new io.trino.sql.ir.Row(elements.build());
                }

                List<io.trino.sql.ir.Expression> arguments = ImmutableList.<io.trino.sql.ir.Expression>builder()
                        .add(elementsRow)
                        .add(node.isNullOnNull() ? io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL : io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL)
                        .build();

                io.trino.sql.ir.Expression function = new io.trino.sql.ir.FunctionCall(resolvedFunction.toQualifiedName(), arguments);

                // apply function to format output
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                io.trino.sql.ir.Expression result = new io.trino.sql.ir.FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(
                        function,
                        new io.trino.sql.ir.GenericLiteral("tinyint", String.valueOf(JsonQuery.EmptyOrErrorBehavior.ERROR.ordinal())),
                        io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new io.trino.sql.ir.Cast(result, treeRewriter.copy(toSqlType(returnedType), context));
                }

                return coerceIfNecessary(node, result, treeRewriter);
            }

            private io.trino.sql.ir.Expression coerceIfNecessary(Expression original, io.trino.sql.ir.Expression rewritten, AstToIrExpressionTreeRewriter<Void> treeRewriter)
            {
                // Don't add a coercion for the top-level expression. That depends on the context the expression is used and it's the responsibility of the caller.
                if (original == expression) {
                    return rewritten;
                }

                Type coercion = analysis.getCoercion(original);
                if (coercion == null) {
                    return rewritten;
                }

                return new io.trino.sql.ir.Cast(
                        rewritten,
                        treeRewriter.copy(toSqlType(coercion), null),
                        false,
                        analysis.isTypeOnlyCoercion(original));
            }
        }, expression, null);
    }

    public static io.trino.sql.ir.Expression copyAstExpressionToIrExpression(Expression expression)
    {
        return AstToIrExpressionTreeRewriter.copyAstNodeToIrNode(expression);
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
            return Optional.of(Symbol.from(outerContext.get().rewriteAstExpressionToIrExpression(expression)));
        }

        return Optional.empty();
    }

    private static void verifyAstExpression(Expression astExpression)
    {
        verify(AstUtils.preOrder(astExpression).noneMatch(expression ->
                expression instanceof SymbolReference ||
                        expression instanceof FunctionCall && ResolvedFunction.isResolved(convertQualifiedName(((FunctionCall) expression).getName()))));
    }

    public Scope getScope()
    {
        return scope;
    }

    //We use a separate method to convert Ast QualifiedName to Ir QualifiedName. Because Ast QualifiedName is used in StatementAnalyzer which involves only with AST, however, some functionalities like
    //MetadataManager only accepts IR QualifiedName, but we should not rely on copyAstExpressionToIrExpression method since AST to IR conversion doesn't happen there, so we create a seperated dedicated method
    // for converting Ast QualifiedName to Ir QualifiedName. In the future, we may split Identifier out of Expression hierarchy such that we don't need this conversion.
    public static io.trino.sql.ir.QualifiedName convertQualifiedName(QualifiedName qualifiedName)
    {
        return io.trino.sql.ir.QualifiedName.of(qualifiedName.getOriginalParts().stream().map(originalPart -> new io.trino.sql.ir.Identifier(originalPart.getValue(), originalPart.isDelimited())).collect(toImmutableList()));
    }

    public static QualifiedName convertQualifiedName(io.trino.sql.ir.QualifiedName qualifiedName)
    {
        return QualifiedName.of(qualifiedName.getOriginalParts().stream().map(originalPart -> new Identifier(originalPart.getValue(), originalPart.isDelimited())).collect(toImmutableList()));
    }

    private static class ParametersRow
    {
        private final Expression parametersRow;
        private final List<String> parametersOrder;

        public ParametersRow(Expression parametersRow, List<String> parametersOrder)
        {
            this.parametersRow = requireNonNull(parametersRow, "parametersRow is null");
            this.parametersOrder = requireNonNull(parametersOrder, "parametersOrder is null");
        }

        public Expression getParametersRow()
        {
            return parametersRow;
        }

        public List<String> getParametersOrder()
        {
            return parametersOrder;
        }
    }

    private static class IrParametersRow
    {
        private final io.trino.sql.ir.Expression parametersRow;
        private final List<String> parametersOrder;

        public IrParametersRow(io.trino.sql.ir.Expression parametersRow, List<String> parametersOrder)
        {
            this.parametersRow = requireNonNull(parametersRow, "parametersRow is null");
            this.parametersOrder = requireNonNull(parametersOrder, "parametersOrder is null");
        }

        public io.trino.sql.ir.Expression getParametersRow()
        {
            return parametersRow;
        }

        public List<String> getParametersOrder()
        {
            return parametersOrder;
        }
    }
}
