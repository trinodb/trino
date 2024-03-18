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
package io.trino.plugin.varada.expression.rewrite;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.MoreCollectors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions;
import io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.NativeExpressionRulesHandler;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.varada.type.TypeUtils.isLongTimeWithTimeZoneType;
import static io.trino.plugin.varada.type.TypeUtils.isLongTimestampType;
import static io.trino.plugin.varada.type.TypeUtils.isLongTimestampTypeWithTimeZoneType;
import static io.trino.plugin.varada.type.TypeUtils.isWarmBasicSupported;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

@Singleton
public class ExpressionService
{
    public static final String PUSHDOWN_PREDICATES_STAT_GROUP = "pushdown-predicates";
    private static final int MAX_TREE_LEVEL = 4;

    private static final Logger logger = Logger.get(ExpressionService.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final SupportedFunctions supportedFunctions;
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private final GlobalConfiguration globalConfiguration;
    private final NativeConfiguration nativeConfiguration;
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    @Inject
    public ExpressionService(
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            SupportedFunctions supportedFunctions,
            GlobalConfiguration globalConfiguration,
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager,
            NativeExpressionRulesHandler nativeExpressionRulesHandler)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.supportedFunctions = requireNonNull(supportedFunctions);
        this.varadaStatsPushdownPredicates = metricsManager.registerMetric(VaradaStatsPushdownPredicates.create(PUSHDOWN_PREDICATES_STAT_GROUP));
        this.nativeExpressionRulesHandler = requireNonNull(nativeExpressionRulesHandler);
    }

    public Optional<WarpExpression> convertToWarpExpression(ConnectorSession session,
                                                            ConnectorExpression expression,
                                                            Map<String, ColumnHandle> assignments,
                                                            Map<String, Long> customStats)
    {
        Optional<WarpExpression> res;
        try {
            if (!VaradaSessionProperties.getEnableOrPushdown(session)) {
                return Optional.empty();
            }
            Set<String> unsupportedFunctions = VaradaSessionProperties.getUnsupportedFunctions(session, globalConfiguration);
            if (unsupportedFunctions.size() == 1 &&
                    unsupportedFunctions.stream().collect(MoreCollectors.onlyElement()).equals("*")) {
                return Optional.empty();
            }
            Optional<VaradaExpression> varadaExpressionOpt = convertToVaradaExpression(session,
                    expression,
                    assignments,
                    unsupportedFunctions,
                    customStats);
            if (varadaExpressionOpt.isPresent()) {
                ImmutableSetMultimap.Builder<RegularColumn, VaradaExpressionData> outVaradaExpressionDataLeaves = ImmutableSetMultimap.builder();
                Set<String> unsupportedNativeFunctions = VaradaSessionProperties.getUnsupportedNativeFunctions(session, nativeConfiguration);

                boolean validExpression = convertToFlatVaradaExpressionDataList(varadaExpressionOpt.get(), outVaradaExpressionDataLeaves, unsupportedNativeFunctions, customStats, 0);
                List<VaradaExpressionData> varadaExpressionDataLeaves = new ArrayList<>(outVaradaExpressionDataLeaves.build().values());

                if (varadaExpressionDataLeaves.isEmpty() ||
                        !validExpression) {
                    res = Optional.empty();
                }
                else {
                    res = Optional.of(new WarpExpression(varadaExpressionOpt.get(), varadaExpressionDataLeaves));
                }
            }
            else {
                res = Optional.empty();
            }
        }
        catch (Exception e) {
            logger.warn("failed to convert expression to warpExpression. expression=%s, error=%s", expression, e.getMessage());
            varadaStatsPushdownPredicates.incfailed_rewrite_expression();
            res = Optional.empty();
        }
        return res;
    }

    private boolean convertToFlatVaradaExpressionDataList(VaradaExpression varadaExpression,
                                                          ImmutableSetMultimap.Builder<RegularColumn, VaradaExpressionData> outVaradaExpressionDataLeaves,
                                                          Set<String> unsupportedNativeFunctions,
                                                          Map<String, Long> customStats,
                                                          int treeLevel)
    {
        if (varadaExpression instanceof VaradaCall varadaCall) {
            if (varadaCall.getFunctionName().equals(OR_FUNCTION_NAME.getName()) ||
                    varadaCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                if (treeLevel == MAX_TREE_LEVEL) {
                    varadaStatsPushdownPredicates.incunsupported_expression_depth();
                    return false;
                }
                treeLevel++;
                for (VaradaExpression expression : varadaCall.getArguments()) {
                    if (!convertToFlatVaradaExpressionDataList(expression, outVaradaExpressionDataLeaves, unsupportedNativeFunctions, customStats, treeLevel)) {
                        return false;
                    }
                }
            }
            else {
                Optional<ColumnHandle> columnHandleOptional = getColumnHandle(varadaExpression);
                if (columnHandleOptional.isEmpty()) {
                    return false;
                }

                Type columnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandleOptional.get());
                RegularColumn varadaColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandleOptional.get());

                Optional<NativeExpression> nativeExpressionOptional;
                if (isNullExpression(varadaExpression)) {
                    nativeExpressionOptional = Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_VALUES,
                            FunctionType.FUNCTION_TYPE_NONE,
                            Domain.onlyNull(columnType),
                            true,
                            true,
                            Collections.emptyList(),
                            TransformFunction.NONE));
                }
                else {
                    nativeExpressionOptional = nativeExpressionRulesHandler.rewrite(varadaExpression, columnType, unsupportedNativeFunctions, customStats);
                }

                Optional<RegularColumn> column;
                if (nativeExpressionOptional.isPresent() &&
                        !Objects.equals(nativeExpressionOptional.get().transformFunction(), TransformFunction.NONE)) {
                    column = getTransformedColumn(varadaColumn, varadaExpression, nativeExpressionOptional.get().transformFunction());
                }
                else {
                    column = Optional.of(varadaColumn);
                }

                if (column.isPresent() && isSupportedColumnType(columnType)) {
                    VaradaExpressionData varadaExpressionData = new VaradaExpressionData(varadaExpression,
                            columnType,
                            nativeExpressionOptional.isPresent() && nativeExpressionOptional.get().collectNulls(),
                            nativeExpressionOptional,
                            column.get());
                    outVaradaExpressionDataLeaves.put(varadaColumn, varadaExpressionData);
                }
            }
        }
        return true;
    }

    private Optional<VaradaExpression> convertToVaradaExpression(ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Set<String> unsupportedFunctions,
            Map<String, Long> customStats)
    {
        Optional<VaradaExpression> res;
        if (expression instanceof Variable variable && isSupportedColumnType(variable.getType())) {
            ColumnHandle columnHandle = assignments.get(variable.getName());
            Type type = variable.getType();
            res = Optional.of(new VaradaVariable(columnHandle, type));
        }
        else if (expression instanceof Call call) {
            FunctionName functionName = call.getFunctionName();
            if (unsupportedFunctions.contains(functionName.getName())) {
                logger.debug("%s function is listed in unsupportedFunctions list. skip", functionName.getName());
                res = Optional.empty();
            }
            else {
                Set<ConnectorExpressionRule<Call, VaradaExpression>> rule = supportedFunctions.getRule(functionName);
                if (rule.isEmpty()) {
                    res = Optional.empty();
                    varadaStatsPushdownPredicates.incunsupported_functions();
                    customStats.compute("unsupported_functions", (key, value) -> value == null ? 1L : value + 1);
                }
                else {
                    ConnectorExpressionRule.RewriteContext<VaradaExpression> context = createContext(assignments, session, unsupportedFunctions, customStats);
                    res = rewrite(rule, expression, context, customStats);
                }
            }
        }
        else if (expression instanceof Constant constant) {
            if (constant.getValue() instanceof Slice) {
                // value of the constant must be typed so a valid serializer/deserializer will be used
                res = Optional.of(new VaradaSliceConstant((Slice) constant.getValue(), constant.getType()));
            }
            else {
                // workaround: cannot use instanceof since JsonPathType is not part of the trino-spi module (different classloader)
                if (constant.getType().getClass().getName().endsWith("JsonPathType")) {
                    // no need to convert the JsonPath object, it's enough to convert only the pattern
                    // use varchar for the type since JsonPath is not part of the trino-spi module
                    res = Optional.of(new VaradaPrimitiveConstant(constant.getValue().toString(), VarcharType.VARCHAR));
                }
                else {
                    res = Optional.of(new VaradaPrimitiveConstant(constant.getValue(), constant.getType()));
                }
            }
        }
        else {
            res = Optional.empty();
        }
        return res;
    }

    private ConnectorExpressionRule.RewriteContext<VaradaExpression> createContext(Map<String, ColumnHandle> assignments,
            ConnectorSession session,
            Set<String> unsupportedFunctions,
            Map<String, Long> customStats)
    {
        return new ConnectorExpressionRule.RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }

            @Override
            public Optional<VaradaExpression> defaultRewrite(ConnectorExpression expression)
            {
                return convertToVaradaExpression(session, expression, assignments, unsupportedFunctions, customStats);
            }
        };
    }

    private Optional<VaradaExpression> rewrite(
            Set<ConnectorExpressionRule<Call, VaradaExpression>> rules,
            ConnectorExpression expression,
            ConnectorExpressionRule.RewriteContext<VaradaExpression> context,
            Map<String, Long> customStats)
    {
        Capture<Call> expressionCapture = newCapture();
        Optional<VaradaExpression> res = Optional.empty();
        boolean anyMatch = false;
        for (ConnectorExpressionRule<Call, VaradaExpression> rule : rules) {
            Pattern<? extends ConnectorExpression> pattern = rule.getPattern().capturedAs(expressionCapture);
            Optional<Match> matches = pattern.match(expression, context).findFirst();
            if (matches.isPresent()) {
                anyMatch = true;
                Match match = matches.get();
                Call capturedExpression = match.capture(expressionCapture);
                verify(Objects.equals(capturedExpression, expression));
                Optional<VaradaExpression> rewritten = rule.rewrite(capturedExpression, match.captures(), context);
                if (rewritten.isPresent()) {
                    res = rewritten;
                    break;
                }
            }
        }
        if (!anyMatch) {
            customStats.compute("unsupported_functions", (key, value) -> value == null ? 1L : value + 1);
            varadaStatsPushdownPredicates.incunsupported_functions();
        }
        return res;
    }

    /**
     * get columnHandle from leaf expression, if a leaf contains 2 column we drop that expression since it not supported
     */
    public static Optional<ColumnHandle> getColumnHandle(VaradaExpression varadaExpression)
    {
        if (varadaExpression instanceof VaradaVariable variable) {
            return Optional.of(variable.getColumnHandle());
        }
        if (varadaExpression instanceof VaradaCall varadaCall) {
            Optional<ColumnHandle> res = Optional.empty();
            for (VaradaExpression child : varadaCall.getArguments()) {
                Optional<ColumnHandle> columnHandle = getColumnHandle(child);
                if (columnHandle.isPresent()) {
                    if (res.isPresent()) {
                        // More than one predicate appears in the expression, not supported
                        return Optional.empty();
                    }
                    else {
                        res = columnHandle;
                    }
                }
            }
            return res;
        }
        return Optional.empty();
    }

    private static boolean isSupportedColumnType(Type columnType)
    {
        boolean res = true;
        if (TypeUtils.isLongDecimalType(columnType)) {
            //todo can't serialize LongDecimalType since Int128 is not serializable
            res = false;
        }
        else if (isLongTimestampType(columnType) || isLongTimestampTypeWithTimeZoneType(columnType) || isLongTimeWithTimeZoneType(columnType)) {
            res = false;
        }
        else if (columnType instanceof MapType mapType) {
            return isSupportedColumnType(mapType.getKeyType()) && isSupportedColumnType(mapType.getValueType()) && isWarmBasicSupported(mapType.getValueType());
        }
        return res;
    }

    private Optional<RegularColumn> getTransformedColumn(RegularColumn regularColumn, VaradaExpression varadaExpression, TransformFunction transformFunction)
    {
        Optional<RegularColumn> res = Optional.empty();

        if (varadaExpression instanceof VaradaCall varadaCall) {
            String functionName = varadaCall.getFunctionName();

            if (supportedFunctions.getComparableStandardFunctions().contains(functionName)) {
                for (VaradaExpression child : varadaCall.getArguments()) {
                    if (child instanceof VaradaCall) {
                        TransformedColumn transformedColumn = new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), transformFunction);
                        res = Optional.of(transformedColumn);
                    }
                }
            }
            else if (functionName.equals(IN_PREDICATE_FUNCTION_NAME.getName()) &&
                    varadaCall.getArguments().get(0) instanceof VaradaCall) {
                TransformedColumn transformedColumn = new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), transformFunction);
                res = Optional.of(transformedColumn);
            }
        }
        return res;
    }

    private boolean isNullExpression(VaradaExpression varadaExpression)
    {
        return varadaExpression instanceof VaradaCall varadaCall && varadaCall.getFunctionName().equals(IS_NULL_FUNCTION_NAME.getName());
    }
}
