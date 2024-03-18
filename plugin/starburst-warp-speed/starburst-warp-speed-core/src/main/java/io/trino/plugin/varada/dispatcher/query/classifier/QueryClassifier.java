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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.varada.storage.engine.ConnectorSync.DEFAULT_CATALOG;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

@Singleton
public class QueryClassifier
{
    private static final Logger logger = Logger.get(QueryClassifier.class);
    public static final int INVALID_TOTAL_RECORDS = -1;
    private final ClassifierFactory classifierFactory;
    private final ConnectorSync connectorSync;
    private final MatchCollectIdService matchCollectIdService;
    private final PredicateContextFactory predicateContextFactory;

    @Inject
    public QueryClassifier(ClassifierFactory classifierFactory,
            ConnectorSync connectorSync,
            MatchCollectIdService matchCollectIdService,
            PredicateContextFactory predicateContextFactory)
    {
        this.classifierFactory = requireNonNull(classifierFactory);
        this.connectorSync = requireNonNull(connectorSync);
        this.matchCollectIdService = requireNonNull(matchCollectIdService);
        this.predicateContextFactory = requireNonNull(predicateContextFactory);
    }

    public QueryContext classifyCache(ImmutableList<ColumnHandle> projectColumns, Optional<UUID> storeIdOpt, RowGroupData rowGroupData)
    {
        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        QueryContext baseQueryContext = new QueryContext(predicateContextData, projectColumns, connectorSync.getCatalogSequence(), false);
        return classify(baseQueryContext,
                rowGroupData,
                null,
                Optional.empty(),
                storeIdOpt,
                ClassificationType.CACHE);
    }

    public QueryContext classify(QueryContext baseQueryContext,
            RowGroupData rowGroupData,
            DispatcherTableHandle dispatcherTableHandle,
            Optional<ConnectorSession> session)
    {
        return classify(
                baseQueryContext,
                rowGroupData,
                dispatcherTableHandle,
                session,
                Optional.empty(),
                ClassificationType.QUERY);
    }

    public QueryContext classify(
            QueryContext baseQueryContext,
            RowGroupData rowGroupData,
            DispatcherTableHandle dispatcherTableHandle,
            Optional<ConnectorSession> session,
            Optional<UUID> storeIdOpt,
            ClassificationType classificationType)
    {
        QueryContext queryContext = null;
        try {
            List<Classifier> classifiers = classifierFactory.getClassifiers(classificationType);
            boolean minMaxFilter = false;
            boolean mappedMatchCollect = false;
            boolean enableInverseWithNulls = false;
            if (session.isPresent()) {
                //in cacheManager we don't have session
                minMaxFilter = VaradaSessionProperties.isMinMaxFilter(session.get());
                mappedMatchCollect = VaradaSessionProperties.getEnabledMappedMatchCollect(session.get());
                enableInverseWithNulls = VaradaSessionProperties.getEnabledInverseWithNulls(session.get());
            }
            WarmedWarmupTypes warmedWarmupTypes = createColumnToWarmUpElementPerType(rowGroupData, storeIdOpt);
            ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                    rowGroupData,
                    baseQueryContext.getPredicateContextData(),
                    baseQueryContext.getRemainingCollectColumnByBlockIndex(),
                    warmedWarmupTypes,
                    minMaxFilter,
                    mappedMatchCollect,
                    enableInverseWithNulls);
            queryContext = baseQueryContext;
            if (classificationType == ClassificationType.QUERY ||
                    classificationType == ClassificationType.CHOOSING_ALTERNATIVE ||
                    classificationType == ClassificationType.CACHE) {
                DispatcherProxiedConnectorTransformer transformer = classifierFactory.getTransformerByType(classificationType);
                int totalRecords = getTotalRecords(classifyArgs, queryContext, transformer);
                queryContext = queryContext.asBuilder()
                        .totalRecords(totalRecords)
                        .build();
            }
            for (Classifier classifier : classifiers) {
                queryContext = classifier.classify(classifyArgs, queryContext);
                if (queryContext.isNoneOnly()) {
                    break;
                }
            }

            if (classificationType == ClassificationType.QUERY) {
                validateAlternative(dispatcherTableHandle, queryContext);
            }

            logger.debug("queryContext::%s", queryContext);
            return queryContext;
        }
        catch (Exception e) {
            if (queryContext != null) {
                close(queryContext);
            }
            throw e;
        }
    }

    private static void validateAlternative(DispatcherTableHandle dispatcherTableHandle, QueryContext queryContext)
    {
        if (!queryContext.isNoneOnly()) {
            List<QueryMatchData> tightnessRequirementIsNotMet = queryContext.getMatchLeavesDFS().stream()
                    .filter(queryMatchData -> queryMatchData.isTightnessRequired() && !queryMatchData.canBeTight())
                    .toList();
            checkState(
                    tightnessRequirementIsNotMet.isEmpty(),
                    "Tightness requirement can't be met for the following queryMatchDatas: %s. dispatcherTableHandle=%s, queryContext=%s",
                    tightnessRequirementIsNotMet,
                    dispatcherTableHandle,
                    queryContext);
        }
    }

    private int getTotalRecords(ClassifyArgs classifyArgs, QueryContext queryContext, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        int totalRecords = INVALID_TOTAL_RECORDS;
        for (RegularColumn regularColumn : queryContext.getPredicateContextData().getRemainingColumns()) {
            totalRecords = getTotalRecords(classifyArgs, queryContext, regularColumn, totalRecords);
        }
        for (ColumnHandle columnHandle : queryContext.getRemainingCollectColumns()) {
            RegularColumn regularColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle);
            totalRecords = getTotalRecords(classifyArgs, queryContext, regularColumn, totalRecords);
        }
        return totalRecords;
    }

    private static int getTotalRecords(ClassifyArgs classifyArgs, QueryContext queryContext, RegularColumn regularColumn, int totalRecords)
    {
        OptionalInt totalRecordsOpt = classifyArgs.getWarmedWarmupTypes().getColumnTotalRecords(regularColumn);
        if (totalRecordsOpt.isPresent()) {
            checkArgument(totalRecordsOpt.getAsInt() == totalRecords || totalRecords == INVALID_TOTAL_RECORDS, "different total records for required WE %s", queryContext);
            totalRecords = totalRecordsOpt.getAsInt();
        }
        return totalRecords;
    }

    public void close(QueryContext queryContext)
    {
        int matchCollectId = queryContext.getMatchCollectId();
        if (matchCollectId != MatchCollectIdService.INVALID_ID) {
            matchCollectIdService.freeMatchCollectId(matchCollectId);
        }
    }

    public QueryContext getBasicQueryContext(List<ColumnHandle> collectColumns,
            DispatcherTableHandle dispatcherTableHandle,
            DynamicFilter dynamicFilter,
            ConnectorSession session)
    {
        return getBasicQueryContext(collectColumns,
                dispatcherTableHandle,
                dynamicFilter,
                session,
                ClassificationType.QUERY);
    }

    public QueryContext getBasicQueryContext(List<ColumnHandle> collectColumns,
            DispatcherTableHandle dispatcherTableHandle,
            DynamicFilter dynamicFilter,
            ConnectorSession session,
            ClassificationType classificationType)
    {
        PredicateContextData predicateContextData = predicateContextFactory.create(session,
                dynamicFilter,
                dispatcherTableHandle);
        String matchCollectCatalog = VaradaSessionProperties.getMatchCollectCatalog(session);
        boolean enableMatchCollect = (classificationType == ClassificationType.QUERY) &&
                VaradaSessionProperties.getEnableMatchCollect(session) &&
                ((matchCollectCatalog == null) ? (connectorSync.getCatalogSequence() == DEFAULT_CATALOG) : matchCollectCatalog.equals(connectorSync.getCatalogName()));
        if (enableMatchCollect) {
            VaradaExpression expression = predicateContextData.getRootExpression();
            if (expression instanceof VaradaCall varadaCall && (varadaCall.getFunctionName().equals(OR_FUNCTION_NAME.getName()) ||
                    varadaCall.getArguments().stream().anyMatch(x -> x instanceof VaradaCall child && child.getFunctionName().equals(OR_FUNCTION_NAME.getName())))) {
                //we cannot support match collect together with OR pushdown. In case OR is pushed we must disable match collect for all elements.
                enableMatchCollect = false;
            }
        }
        return new QueryContext(predicateContextData,
                ImmutableList.copyOf(collectColumns),
                connectorSync.getCatalogSequence(),
                enableMatchCollect);
    }

    private WarmedWarmupTypes createColumnToWarmUpElementPerType(RowGroupData rowGroupData, Optional<UUID> storeIdOpt)
    {
        WarmedWarmupTypes.Builder builder = new WarmedWarmupTypes.Builder();
        if (storeIdOpt.isPresent()) {
            UUID storeId = storeIdOpt.get();
            for (WarmUpElement we : rowGroupData.getValidWarmUpElements()) {
                if (storeId.equals(we.getStoreId())) {
                    builder.add(we);
                }
            }
        }
        else {
            for (WarmUpElement we : rowGroupData.getValidWarmUpElements()) {
                builder.add(we);
            }
        }

        return builder.build();
    }

    public Optional<UUID> getQueryStoreId(RowGroupData rowGroupData, List<CacheColumnId> requiredColumns)
    {
        if (rowGroupData.getValidWarmUpElements().size() < requiredColumns.size()) {
            return Optional.empty();
        }

        Map<VaradaColumn, Set<UUID>> columnToWeStoreIds = new HashMap<>();
        for (WarmUpElement warmUpElement : rowGroupData.getValidWarmUpElements()) {
            columnToWeStoreIds
                    .computeIfAbsent(warmUpElement.getVaradaColumn(), _ -> new HashSet<>())
                    .add(warmUpElement.getStoreId());
        }

        RegularColumn pickedColumn = new RegularColumn(requiredColumns.getFirst().toString());
        Set<UUID> storeIds = columnToWeStoreIds.getOrDefault(pickedColumn, Collections.emptySet());
        for (int i = 1; i < requiredColumns.size() && !storeIds.isEmpty(); i++) {
            pickedColumn = new RegularColumn(requiredColumns.get(i).toString());
            Set<UUID> columnStoreIds = columnToWeStoreIds.getOrDefault(pickedColumn, Collections.emptySet());
            storeIds.retainAll(columnStoreIds);
        }
        return storeIds.stream().findFirst();
    }
}
