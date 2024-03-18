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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.plugin.varada.util.DomainUtils;
import io.trino.plugin.varada.util.SimplifyResult;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.varada.type.TypeUtils.isWarmBasicSupported;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class PredicateContextFactory
{
    private final GlobalConfiguration globalConfiguration;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    @Inject
    public PredicateContextFactory(GlobalConfiguration globalConfiguration, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.globalConfiguration = globalConfiguration;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    PredicateContextData create(ConnectorSession session,
            DynamicFilter dynamicFilter,
            DispatcherTableHandle dispatcherTableHandle)
    {
        Optional<WarpExpression> warpExpression = dispatcherTableHandle.getWarpExpression();
        if (!(dynamicFilter.getCurrentPredicate().isAll() || dynamicFilter.getCurrentPredicate().isNone())) {
            //in case we have dynamicFilter we can't use warpExpression. we probably need to intersect the expression as well with DF.
            warpExpression = Optional.empty();
        }
        TupleDomain<ColumnHandle> intersectTupleDomain = dispatcherTableHandle.getFullPredicate()
                .intersect(dynamicFilter.getCurrentPredicate());
        if (intersectTupleDomain.isNone()) {
            return new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.FALSE);
        }
        int predicateThreshold = VaradaSessionProperties.getPredicateSimplifyThreshold(session, globalConfiguration);

        SimplifyResult<ColumnHandle> simplifyResult = DomainUtils.simplify(intersectTupleDomain, predicateThreshold);
        Set<RegularColumn> simplifiedColumns = Stream.concat(dispatcherTableHandle.getSimplifiedColumns().simplifiedColumns().stream(),
                        simplifyResult.getSimplifiedColumns().stream().map(dispatcherProxiedConnectorTransformer::getVaradaRegularColumn))
                .collect(Collectors.toSet());
        TupleDomain<ColumnHandle> tupleDomain = simplifyResult.getTupleDomain();
        return create(warpExpression, tupleDomain, simplifiedColumns);
    }

    private PredicateContextData create(Optional<WarpExpression> warpExpression,
            TupleDomain<ColumnHandle> tupleDomain,
            Set<RegularColumn> simplifiedColumns)
    {
        ImmutableMap.Builder<VaradaExpression, PredicateContext> predicateContextMap = ImmutableMap.builder();
        warpExpression.ifPresent((expression) -> {
            for (VaradaExpressionData leaf : expression.varadaExpressionDataLeaves()) {
                PredicateContext predicateContext = new PredicateContext(leaf);
                predicateContextMap.put(leaf.getExpression(), predicateContext);
            }
        });
        List<VaradaExpression> domainExpressions = new ArrayList<>();
        tupleDomain.getDomains().ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((columnHandle, domain) -> {
            Type columnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandle);
            if (isWarmBasicSupported(columnType)) {
                VaradaVariable varadaVariable = new VaradaVariable(columnHandle, domain.getType());
                RegularColumn varadaColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle);
                VaradaExpression varadaExpression = new DomainExpression(varadaVariable, domain);
                boolean isSimplified = simplifiedColumns.contains(varadaColumn);
                PredicateType predicateType = PredicateUtil.calcPredicateType(domain, columnType); // todo: move ClassifyArgs::getPredicateTypeFromCache to a global cache?
                NativeExpression nativeExpression = NativeExpression.builder()
                        .predicateType(predicateType)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain)
                        .collectNulls(domain.isNullAllowed())
                        .build();
                VaradaExpressionData varadaExpressionData = new VaradaExpressionData(varadaExpression,
                        columnType,
                        domain.isNullAllowed(),
                        Optional.of(nativeExpression),
                        varadaColumn);
                PredicateContext predicateContext = new PredicateContext(varadaExpressionData, isSimplified);
                domainExpressions.add(varadaExpression);
                predicateContextMap.put(varadaExpression, predicateContext);
            }
        }));
        VaradaExpression rootExpression = VaradaPrimitiveConstant.TRUE;

        if (warpExpression.isPresent() && domainExpressions.size() > 0) {
            VaradaExpression warpRootExpression = warpExpression.get().rootExpression();
            if (warpRootExpression instanceof VaradaCall varadaCall && varadaCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                domainExpressions.addAll(warpRootExpression.getChildren());
            }
            else {
                domainExpressions.add(warpRootExpression);
            }
            rootExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), domainExpressions, BOOLEAN);
        }
        else if (warpExpression.isPresent()) {
            rootExpression = warpExpression.get().rootExpression();
        }
        else if (domainExpressions.size() > 0) {
            if (domainExpressions.size() == 1) {
                rootExpression = domainExpressions.get(0);
            }
            else {
                rootExpression = new VaradaCall(AND_FUNCTION_NAME.getName(), domainExpressions, BOOLEAN);
            }
        }
        return new PredicateContextData(predicateContextMap.buildOrThrow(), rootExpression);
    }
}
