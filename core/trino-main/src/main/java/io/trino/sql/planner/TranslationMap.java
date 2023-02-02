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
import io.trino.operator.scalar.ArrayConstructor;
import io.trino.operator.scalar.FormatFunction;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ExpressionAnalyzer.LabelPrefixedReference;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.util.AstUtils;
import io.trino.type.FunctionType;
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
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.KEEP;
import static io.trino.sql.tree.JsonQuery.QuotesBehavior.OMIT;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.util.Failures.checkCondition;
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

    public Expression rewrite(Expression expression)
    {
        verifyAstExpression(expression);

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }

            @Override
            public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
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
                Optional<SymbolReference> mapped = tryGetMapping(node);
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
                        .orElseGet(() -> coerceIfNecessary(node, node));
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
                    Optional<SymbolReference> mapped = tryGetMapping(node);
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

                Optional<SymbolReference> mapped = tryGetMapping(node);
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
            public Expression rewriteArray(Array node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                checkCondition(node.getValues().size() <= 254, TOO_MANY_ARGUMENTS, "Too many arguments for array constructor");

                List<Type> types = node.getValues().stream()
                        .map(analysis::getType)
                        .collect(toImmutableList());

                List<Expression> values = node.getValues().stream()
                        .map(element -> treeRewriter.rewrite(element, context))
                        .collect(toImmutableList());

                FunctionCall call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of(ArrayConstructor.NAME))
                        .setArguments(types, values)
                        .build();

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteCurrentCatalog(CurrentCatalog node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                return coerceIfNecessary(node, new FunctionCall(
                        plannerContext.getMetadata()
                                .resolveFunction(session, QualifiedName.of("$current_catalog"), ImmutableList.of())
                                .toQualifiedName(),
                        ImmutableList.of()));
            }

            @Override
            public Expression rewriteCurrentSchema(CurrentSchema node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                return coerceIfNecessary(node, new FunctionCall(
                        plannerContext.getMetadata()
                                .resolveFunction(session, QualifiedName.of("$current_schema"), ImmutableList.of())
                                .toQualifiedName(),
                        ImmutableList.of()));
            }

            @Override
            public Expression rewriteCurrentPath(CurrentPath node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                return coerceIfNecessary(node, new FunctionCall(
                        plannerContext.getMetadata()
                                .resolveFunction(session, QualifiedName.of("$current_path"), ImmutableList.of())
                                .toQualifiedName(),
                        ImmutableList.of()));
            }

            @Override
            public Expression rewriteCurrentUser(CurrentUser node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                return coerceIfNecessary(node, new FunctionCall(
                        plannerContext.getMetadata()
                                .resolveFunction(session, QualifiedName.of("$current_user"), ImmutableList.of())
                                .toQualifiedName(),
                        ImmutableList.of()));
            }

            @Override
            public Expression rewriteCurrentTime(CurrentTime node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                FunctionCall call = switch (node.getFunction()) {
                    case DATE -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("current_date"))
                            .build();
                    case TIME -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$current_time"))
                            .setArguments(ImmutableList.of(analysis.getType(node)), ImmutableList.of(new NullLiteral()))
                            .build();
                    case LOCALTIME -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$localtime"))
                            .setArguments(ImmutableList.of(analysis.getType(node)), ImmutableList.of(new NullLiteral()))
                            .build();
                    case TIMESTAMP -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$current_timestamp"))
                            .setArguments(ImmutableList.of(analysis.getType(node)), ImmutableList.of(new NullLiteral()))
                            .build();
                    case LOCALTIMESTAMP -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$localtimestamp"))
                            .setArguments(ImmutableList.of(analysis.getType(node)), ImmutableList.of(new NullLiteral()))
                            .build();
                };

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Expression value = treeRewriter.rewrite(node.getExpression(), context);
                Type type = analysis.getType(node.getExpression());

                FunctionCall call = switch (node.getField()) {
                    case YEAR -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("year"))
                            .addArgument(type, value)
                            .build();
                    case QUARTER -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("quarter"))
                            .addArgument(type, value)
                            .build();
                    case MONTH -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("month"))
                            .addArgument(type, value)
                            .build();
                    case WEEK -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("week"))
                            .addArgument(type, value)
                            .build();
                    case DAY, DAY_OF_MONTH -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("day"))
                            .addArgument(type, value)
                            .build();
                    case DAY_OF_WEEK, DOW -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("day_of_week"))
                            .addArgument(type, value)
                            .build();
                    case DAY_OF_YEAR, DOY -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("day_of_year"))
                            .addArgument(type, value)
                            .build();
                    case YEAR_OF_WEEK, YOW -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("year_of_week"))
                            .addArgument(type, value)
                            .build();
                    case HOUR -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("hour"))
                            .addArgument(type, value)
                            .build();
                    case MINUTE -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("minute"))
                            .addArgument(type, value)
                            .build();
                    case SECOND -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("second"))
                            .addArgument(type, value)
                            .build();
                    case TIMEZONE_MINUTE -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("timezone_minute"))
                            .addArgument(type, value)
                            .build();
                    case TIMEZONE_HOUR -> FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("timezone_hour"))
                            .addArgument(type, value)
                            .build();
                };

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteAtTimeZone(AtTimeZone node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Type valueType = analysis.getType(node.getValue());
                Expression value = treeRewriter.rewrite(node.getValue(), context);

                Type timeZoneType = analysis.getType(node.getTimeZone());
                Expression timeZone = treeRewriter.rewrite(node.getTimeZone(), context);

                FunctionCall call;
                if (valueType instanceof TimeType type) {
                    call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$at_timezone"))
                            .addArgument(createTimeWithTimeZoneType(type.getPrecision()), new Cast(value, toSqlType(createTimeWithTimeZoneType(((TimeType) valueType).getPrecision()))))
                            .addArgument(timeZoneType, timeZone)
                            .build();
                }
                else if (valueType instanceof TimeWithTimeZoneType) {
                    call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("$at_timezone"))
                            .addArgument(valueType, value)
                            .addArgument(timeZoneType, timeZone)
                            .build();
                }
                else if (valueType instanceof TimestampType type) {
                    call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("at_timezone"))
                            .addArgument(createTimestampWithTimeZoneType(type.getPrecision()), new Cast(value, toSqlType(createTimestampWithTimeZoneType(((TimestampType) valueType).getPrecision()))))
                            .addArgument(timeZoneType, timeZone)
                            .build();
                }
                else if (valueType instanceof TimestampWithTimeZoneType) {
                    call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of("at_timezone"))
                            .addArgument(valueType, value)
                            .addArgument(timeZoneType, timeZone)
                            .build();
                }
                else {
                    throw new IllegalArgumentException("Unexpected type: " + valueType);
                }

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteFormat(Format node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                List<Expression> arguments = node.getArguments().stream()
                        .map(value -> treeRewriter.rewrite(value, context))
                        .collect(toImmutableList());
                List<Type> argumentTypes = node.getArguments().stream()
                        .map(analysis::getType)
                        .collect(toImmutableList());

                FunctionCall call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of(FormatFunction.NAME))
                        .addArgument(VARCHAR, arguments.get(0))
                        .addArgument(RowType.anonymous(argumentTypes.subList(1, arguments.size())), new Row(arguments.subList(1, arguments.size())))
                        .build();

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteTryExpression(TryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Type type = analysis.getType(node);
                Expression expression = treeRewriter.rewrite(node.getInnerExpression(), context);

                FunctionCall call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of(TryFunction.NAME))
                        .addArgument(new FunctionType(ImmutableList.of(), type), new LambdaExpression(ImmutableList.of(), expression))
                        .build();

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteLikePredicate(LikePredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                Expression value = treeRewriter.rewrite(node.getValue(), context);
                Expression pattern = treeRewriter.rewrite(node.getPattern(), context);
                Optional<Expression> escape = node.getEscape().map(e -> treeRewriter.rewrite(e, context));

                FunctionCall patternCall;
                if (escape.isPresent()) {
                    patternCall = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of(LIKE_PATTERN_FUNCTION_NAME))
                            .addArgument(analysis.getType(node.getPattern()), pattern)
                            .addArgument(analysis.getType(node.getEscape().get()), escape.get())
                            .build();
                }
                else {
                    patternCall = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                            .setName(QualifiedName.of(LIKE_PATTERN_FUNCTION_NAME))
                            .addArgument(analysis.getType(node.getPattern()), pattern)
                            .build();
                }

                FunctionCall call = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of(LIKE_FUNCTION_NAME))
                        .addArgument(analysis.getType(node.getValue()), value)
                        .addArgument(LIKE_PATTERN, patternCall)
                        .build();

                return coerceIfNecessary(node, call);
            }

            @Override
            public Expression rewriteTrim(Trim node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
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
                Optional<SymbolReference> mapped = tryGetMapping(node);
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
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                checkState(analysis.getParameters().size() > node.getId(), "Too few parameter values");
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

            @Override
            public Expression rewriteJsonExists(JsonExists node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonExists rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonExists.ErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                Expression input = new FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));

                Expression result = new FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                return coerceIfNecessary(node, result);
            }

            @Override
            public Expression rewriteJsonValue(JsonValue node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression, default expressions, and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonValue rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonValue.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                Expression input = new FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(rewritten.getEmptyDefault().orElseGet(() -> new Cast(new NullLiteral(), toSqlType(resolvedFunction.getSignature().getReturnType()))))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())))
                        .add(rewritten.getErrorDefault().orElseGet(() -> new Cast(new NullLiteral(), toSqlType(resolvedFunction.getSignature().getReturnType()))));

                Expression result = new FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                return coerceIfNecessary(node, result);
            }

            @Override
            public Expression rewriteJsonQuery(JsonQuery node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonQuery rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonQuery.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                ResolvedFunction inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                Expression input = new FunctionCall(inputToJson.toQualifiedName(), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunction.getSignature().getArgumentType(2),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, plannerContext).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(plannerContext).toExpression(session, path, plannerContext.getTypeManager().getType(TypeId.of(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getWrapperBehavior().ordinal())))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));

                Expression function = new FunctionCall(resolvedFunction.toQualifiedName(), arguments.build());

                // apply function to format output
                GenericLiteral errorBehavior = new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal()));
                BooleanLiteral omitQuotes = new BooleanLiteral(node.getQuotesBehavior().orElse(KEEP) == OMIT ? "true" : "false");
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                Expression result = new FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(function, errorBehavior, omitQuotes));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new Cast(result, toSqlType(returnedType));
                }

                return coerceIfNecessary(node, result);
            }

            private ParametersRow getParametersRow(
                    List<JsonPathParameter> pathParameters,
                    List<JsonPathParameter> rewrittenPathParameters,
                    Type parameterRowType,
                    BooleanLiteral failOnError)
            {
                Expression parametersRow;
                List<String> parametersOrder;
                if (!pathParameters.isEmpty()) {
                    ImmutableList.Builder<Expression> parameters = ImmutableList.builder();
                    for (int i = 0; i < pathParameters.size(); i++) {
                        ResolvedFunction parameterToJson = analysis.getJsonInputFunction(pathParameters.get(i).getParameter());
                        Expression rewrittenParameter = rewrittenPathParameters.get(i).getParameter();
                        if (parameterToJson != null) {
                            parameters.add(new FunctionCall(parameterToJson.toQualifiedName(), ImmutableList.of(rewrittenParameter, failOnError)));
                        }
                        else {
                            parameters.add(rewrittenParameter);
                        }
                    }
                    parametersRow = new Cast(new Row(parameters.build()), toSqlType(parameterRowType));
                    parametersOrder = pathParameters.stream()
                            .map(parameter -> parameter.getName().getCanonicalValue())
                            .collect(toImmutableList());
                }
                else {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(parameterRowType), "invalid type of parameters row when no parameters are passed");
                    parametersRow = new Cast(new NullLiteral(), toSqlType(JSON_NO_PARAMETERS_ROW_TYPE));
                    parametersOrder = ImmutableList.of();
                }

                return new ParametersRow(parametersRow, parametersOrder);
            }

            @Override
            public Expression rewriteJsonObject(JsonObject node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                Expression keysRow;
                Expression valuesRow;

                // prepare keys and values as rows
                if (node.getMembers().isEmpty()) {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(0)));
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(1)));
                    keysRow = new Cast(new NullLiteral(), toSqlType(JSON_NO_PARAMETERS_ROW_TYPE));
                    valuesRow = new Cast(new NullLiteral(), toSqlType(JSON_NO_PARAMETERS_ROW_TYPE));
                }
                else {
                    ImmutableList.Builder<Expression> keys = ImmutableList.builder();
                    ImmutableList.Builder<Expression> values = ImmutableList.builder();
                    for (JsonObjectMember member : node.getMembers()) {
                        Expression key = member.getKey();
                        Expression value = member.getValue();

                        Expression rewrittenKey = treeRewriter.rewrite(key, context);
                        keys.add(rewrittenKey);

                        Expression rewrittenValue = treeRewriter.rewrite(value, context);
                        ResolvedFunction valueToJson = analysis.getJsonInputFunction(value);
                        if (valueToJson != null) {
                            values.add(new FunctionCall(valueToJson.toQualifiedName(), ImmutableList.of(rewrittenValue, TRUE_LITERAL)));
                        }
                        else {
                            values.add(rewrittenValue);
                        }
                    }
                    keysRow = new Row(keys.build());
                    valuesRow = new Row(values.build());
                }

                List<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(keysRow)
                        .add(valuesRow)
                        .add(node.isNullOnNull() ? TRUE_LITERAL : FALSE_LITERAL)
                        .add(node.isUniqueKeys() ? TRUE_LITERAL : FALSE_LITERAL)
                        .build();

                Expression function = new FunctionCall(resolvedFunction.toQualifiedName(), arguments);

                // apply function to format output
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                Expression result = new FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(
                        function,
                        new GenericLiteral("tinyint", String.valueOf(JsonQuery.EmptyOrErrorBehavior.ERROR.ordinal())),
                        FALSE_LITERAL));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new Cast(result, toSqlType(returnedType));
                }

                return coerceIfNecessary(node, result);
            }

            @Override
            public Expression rewriteJsonArray(JsonArray node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<SymbolReference> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                Expression elementsRow;

                // prepare elements as row
                if (node.getElements().isEmpty()) {
                    checkState(JSON_NO_PARAMETERS_ROW_TYPE.equals(resolvedFunction.getSignature().getArgumentType(0)));
                    elementsRow = new Cast(new NullLiteral(), toSqlType(JSON_NO_PARAMETERS_ROW_TYPE));
                }
                else {
                    ImmutableList.Builder<Expression> elements = ImmutableList.builder();
                    for (JsonArrayElement arrayElement : node.getElements()) {
                        Expression element = arrayElement.getValue();
                        Expression rewrittenElement = treeRewriter.rewrite(element, context);
                        ResolvedFunction elementToJson = analysis.getJsonInputFunction(element);
                        if (elementToJson != null) {
                            elements.add(new FunctionCall(elementToJson.toQualifiedName(), ImmutableList.of(rewrittenElement, TRUE_LITERAL)));
                        }
                        else {
                            elements.add(rewrittenElement);
                        }
                    }
                    elementsRow = new Row(elements.build());
                }

                List<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(elementsRow)
                        .add(node.isNullOnNull() ? TRUE_LITERAL : FALSE_LITERAL)
                        .build();

                Expression function = new FunctionCall(resolvedFunction.toQualifiedName(), arguments);

                // apply function to format output
                ResolvedFunction outputFunction = analysis.getJsonOutputFunction(node);
                Expression result = new FunctionCall(outputFunction.toQualifiedName(), ImmutableList.of(
                        function,
                        new GenericLiteral("tinyint", String.valueOf(JsonQuery.EmptyOrErrorBehavior.ERROR.ordinal())),
                        FALSE_LITERAL));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(TypeSignatureTranslator::toTypeSignature)
                        .map(plannerContext.getTypeManager()::getType)
                        .orElse(VARCHAR);

                Type resultType = outputFunction.getSignature().getReturnType();
                if (!resultType.equals(returnedType)) {
                    result = new Cast(result, toSqlType(returnedType));
                }

                return coerceIfNecessary(node, result);
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

    private Optional<SymbolReference> tryGetMapping(Expression expression)
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
}
