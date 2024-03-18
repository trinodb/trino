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
package io.trino.plugin.varada.warmup;

import com.google.common.eventbus.EventBus;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.di.FakeConnectorSessionProvider;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.dispatcher.warmup.events.WarmRulesChangedEvent;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.plugin.varada.warmup.dal.WarmupRuleDao;
import io.trino.plugin.varada.warmup.model.WarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.varada.warmup.model.WarmupRuleResult;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.varada.tools.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

@Singleton
public class WarmupRuleService
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(WarmupRuleService.class);

    public static final String WARMUP_PATH = "warmup";
    public static final String TASK_NAME_GET = "warmup-rule-get";

    private final Connector proxiedConnector;
    private final StorageEngineConstants storageEngineConstants;
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final FakeConnectorSessionProvider fakeConnectorSessionProvider;
    private final WarmupRuleDao warmupRuleDao;
    private final EventBus eventBus;

    protected final ReadWriteLock readWriteLock;
    private final GlobalConfiguration globalConfiguration;

    @Inject
    public WarmupRuleService(@ForWarp Connector proxiedConnector,
                             StorageEngineConstants storageEngineConstants,
                             WarmupDemoterConfiguration warmupDemoterConfiguration,
                             DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
                             FakeConnectorSessionProvider fakeConnectorSessionProvider,
                             WarmupRuleDao warmupRuleDao,
                             EventBus eventBus,
                             VaradaInitializedServiceRegistry varadaInitializedServiceRegistry,
                             GlobalConfiguration globalConfiguration)
    {
        this.proxiedConnector = requireNonNull(proxiedConnector);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.warmupDemoterConfiguration = requireNonNull(warmupDemoterConfiguration);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.fakeConnectorSessionProvider = requireNonNull(fakeConnectorSessionProvider);
        this.warmupRuleDao = requireNonNull(warmupRuleDao);
        this.eventBus = requireNonNull(eventBus);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        varadaInitializedServiceRegistry.addService(this);

        this.readWriteLock = new ReentrantReadWriteLock();
    }

    @Override
    public void init()
    {
        readWriteLock.writeLock().lock();
        try {
            Collection<WarmupRule> allRules = getAll();
            logger.debug("total rules size=%s", allRules.size());
        }
        catch (Exception e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "failed to init warmupRules",
                    e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public synchronized WarmupRuleResult save(List<WarmupRule> newWarmupRules)
            throws TrinoException
    {
        readWriteLock.writeLock().lock();
        try {
            WarmupRuleResult warmupRuleResult = validate(getAll(), newWarmupRules);
            List<WarmupRule> appliedRules = Collections.emptyList();
            if (!warmupRuleResult.appliedRules().isEmpty()) {
                warmupRuleDao.save(warmupRuleResult.appliedRules());
                notifyWarmRulesChangedEvent();
                appliedRules = warmupRuleResult.appliedRules();
            }
            return new WarmupRuleResult(appliedRules, warmupRuleResult.rejectedRules());
        }
        catch (Exception e) {
            logger.error(e, "failed to save new warmupRules=%s", newWarmupRules);
            throw new TrinoException(VaradaErrorCode.VARADA_RULE_CONFIGURATION_ERROR,
                    "failed to save new warmupRules",
                    e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public synchronized WarmupRuleResult replaceAll(List<WarmupRule> newWarmupRules)
            throws TrinoException
    {
        readWriteLock.writeLock().lock();
        try {
            WarmupRuleResult warmupRuleResult = validate(Collections.emptyList(), newWarmupRules);
            List<WarmupRule> appliedRules = warmupRuleDao.replaceAll(warmupRuleResult.appliedRules());
            notifyWarmRulesChangedEvent();
            appliedRules = (appliedRules == null) ? Collections.emptyList() : appliedRules;
            return new WarmupRuleResult(appliedRules, warmupRuleResult.rejectedRules());
        }
        catch (Exception e) {
            String error = String.format("failed to replace existing rules with new rules=%s", newWarmupRules);
            throw new TrinoException(VaradaErrorCode.VARADA_RULE_CONFIGURATION_ERROR, error, e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public WarmupRuleResult validate(Collection<WarmupRule> existingWarmupRules, Collection<WarmupRule> newWarmupRules)
    {
        WarmupRuleResult warmupRuleResult = validateNewRules(existingWarmupRules, newWarmupRules);
        if (!warmupRuleResult.rejectedRules().isEmpty()) {
            logger.error(warmupRuleResult.rejectedRules().toString());
        }
        return warmupRuleResult;
    }

    private WarmupRuleResult validateNewRules(Collection<WarmupRule> existingWarmupRules, Collection<WarmupRule> newWarmupRules)
    {
        Map<WarmupRule, Set<String>> rejectedRules = new HashMap<>();
        List<WarmupRule> appliedRules = new ArrayList<>();

        Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata = getTableColumnsMetadata(newWarmupRules);

        Map<String, List<WarmupRule>> existingRuleByColumn = existingWarmupRules.stream().collect(groupingBy(this::getColumnUniqueKey));

        Set<Long> existingUniqueRuleIds = existingWarmupRules.stream()
                .map(this::getUniqueRuleId)
                .collect(Collectors.toSet());
        Set<Long> uniqueIds = new HashSet<>();

        Set<Integer> existingRuleIds = existingWarmupRules.stream()
                .map(WarmupRule::getId)
                .collect(Collectors.toSet());
        for (WarmupRule warmupRule : newWarmupRules) {
            long uniqueRuleId = getUniqueRuleId(warmupRule);
            if (uniqueIds.contains(uniqueRuleId) || (warmupRule.getId() == 0 && existingUniqueRuleIds.contains(uniqueRuleId))) {
                String error = String.format("%d: can't add 2 rules with the same key. rules=%s",
                        VaradaErrorCode.VARADA_DUPLICATE_RECORD.getCode(),
                        warmupRule);
                rejectedRules.computeIfAbsent(warmupRule, e -> new HashSet<>()).add(error);
                continue;
            }
            else {
                uniqueIds.add(uniqueRuleId);
            }
            if (warmupRule.getId() != 0 && !existingRuleIds.contains(warmupRule.getId())) {
                String error = String.format("%d: New Rule id must be 0, or override an existing rule id",
                        VaradaErrorCode.VARADA_WARMUP_RULE_ID_NOT_VALID.getCode());
                rejectedRules.computeIfAbsent(warmupRule, e -> new HashSet<>()).add(error);
                continue;
            }
            Map<String, ColumnMetadata> tableColumnsMetadata = getTableColumnsMetadata(tablesColumnsMetadata, warmupRule);
            if (tableColumnsMetadata == null) {
                String error = String.format("%d: Rule refer to a non-exist table (%s)",
                        VaradaErrorCode.VARADA_WARMUP_RULE_UNKNOWN_TABLE.getCode(),
                        getSchemaTableName(warmupRule));
                rejectedRules.computeIfAbsent(warmupRule, e -> new HashSet<>()).add(error);
            }
            else {
                Optional<Type> columnType = getColumnType(tableColumnsMetadata, warmupRule);
                if (columnType.isEmpty() && !(warmupRule.getVaradaColumn() instanceof WildcardColumn)) {
                    String error = String.format("%d: Rule refer to a non-exist column (%s)",
                            VaradaErrorCode.VARADA_WARMUP_RULE_UNKNOWN_COLUMN.getCode(),
                            warmupRule.getVaradaColumn().getName());
                    rejectedRules.computeIfAbsent(warmupRule, e -> new HashSet<>()).add(error);
                }
                else {
                    String columnUniqueKey = getColumnUniqueKey(warmupRule);
                    boolean valid = validateRule(rejectedRules, warmupRule, columnUniqueKey, existingRuleByColumn, columnType);
                    if (valid) {
                        List<WarmupRule> columnRules = existingRuleByColumn.computeIfAbsent(columnUniqueKey, rule -> new ArrayList<>());
                        columnRules.add(warmupRule); // adding to find failures in the list to be saved
                        appliedRules.add(warmupRule);
                    }
                }
            }
        }
        return new WarmupRuleResult(appliedRules, rejectedRules);
    }

    private boolean validateRule(Map<WarmupRule, Set<String>> rejectedRules,
            WarmupRule warmupRule,
            String columnUniqueKey,
            Map<String, List<WarmupRule>> existingRuleByColumn,
            Optional<Type> optionalType)
    {
        Set<String> errors = new HashSet<>();
        if (!(warmupRule.getVaradaColumn() instanceof WildcardColumn)) {
            if (optionalType.isPresent()) {
                Type type = optionalType.get();
                if (warmupRule.getPriority() >= warmupDemoterConfiguration.getDefaultRulePriority()) {
                    validateTypeIsSupported(errors, warmupRule, type);
                    validateMaxCharLength(errors, warmupRule, type);
                }
                validateBloomIndexLength(errors, warmupRule, type);
                validateSingleBloom(errors, warmupRule, columnUniqueKey, existingRuleByColumn);
                validateDataOnly(errors, warmupRule);
            }
            else {
                errors.add("WarmUpType is null");
            }
            if (!errors.isEmpty()) {
                rejectedRules.put(warmupRule, errors);
                return false;
            }
        }
        return true;
    }

    private void validateTypeIsSupported(Set<String> errors, WarmupRule warmupRule, Type type)
    {
        boolean fail = false;
        if (WarmUpType.WARM_UP_TYPE_LUCENE.equals(warmupRule.getWarmUpType())) {
            if (!TypeUtils.isWarmLuceneSupported(type)) {
                fail = true;
            }
        }
        else if (TypeUtils.isRowType(type) || TypeUtils.isMapType(type) ||
                (!WarmUpType.WARM_UP_TYPE_DATA.equals(warmupRule.getWarmUpType()) && (TypeUtils.isArrayType(type) || TypeUtils.isJsonType(type)))) {
            fail = true;
        }
        else if (WarmUpType.WARM_UP_TYPE_DATA.equals(warmupRule.getWarmUpType()) && TypeUtils.isArrayType(type)) {
            ArrayType arrayType = (ArrayType) type;
            if (TypeUtils.isRowType(arrayType.getElementType())) {
                fail = true;
            }
        }

        if (fail) {
            String error = String.format("%d: Warmup type %s doesn't support column type %s",
                    VaradaErrorCode.VARADA_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode(),
                    warmupRule.getWarmUpType(),
                    type);
            errors.add(error);
        }
    }

    private void validateMaxCharLength(Set<String> errors, WarmupRule warmupRule, Type type)
    {
        if (warmupRule.getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA &&
                TypeUtils.isCharType(type) && ((CharType) type).getLength() > storageEngineConstants.getMaxRecLen()) {
            String error = String.format("%d: Can't warm long Char columns. maximum of %d is allowed",
                    VaradaErrorCode.VARADA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH.getCode(),
                    storageEngineConstants.getMaxRecLen());
            errors.add(error);
        }
    }

    private void validateBloomIndexLength(Set<String> errors, WarmupRule warmupRule, Type type)
    {
        if (warmupRule.getWarmUpType().bloom() &&
                (TypeUtils.getIndexTypeLength(type, storageEngineConstants.getFixedLengthStringLimit()) < Integer.BYTES)) {
            String error = String.format("%d: Bloom index can be applied only to types that use index length smaller than %d",
                    VaradaErrorCode.VARADA_WARMUP_RULE_BLOOM_ILLEGAL_TYPE.getCode(), Integer.BYTES);
            errors.add(error);
        }
    }

    private void validateSingleBloom(Set<String> errors, WarmupRule warmupRule, String columnUniqueKey, Map<String, List<WarmupRule>> existingRuleByColumn)
    {
        if (warmupRule.getWarmUpType().bloom()) {
            List<WarmupRule> columnRules = existingRuleByColumn.get(columnUniqueKey);
            if (columnRules != null && columnRules.stream().anyMatch(rule -> rule.getWarmUpType().bloom())) {
                String error = String.format("%d: Only single bloom index allowed",
                        VaradaErrorCode.VARADA_WARMUP_RULE_BLOOM_TOO_MANY_BLOOM_WARMUPS.getCode());
                errors.add(error);
            }
        }
    }

    private void validateDataOnly(Set<String> errors, WarmupRule warmupRule)
    {
        if (globalConfiguration.isDataOnlyWarming() && !warmupRule.getWarmUpType().equals(WarmUpType.WARM_UP_TYPE_DATA)) {
            errors.add(String.format("%d: creation of %s warmUpType is not supported with Local Data Storage connector",
                    VaradaErrorCode.VARADA_INDEX_WARMUP_RULE_IS_NOT_ALLOWED.getCode(),
                    warmupRule.getWarmUpType()));
        }
    }

    public List<WarmupRule> getAll()
    {
        return warmupRuleDao.getAll();
    }

    public void delete(List<Integer> ids)
    {
        readWriteLock.writeLock().lock();
        try {
            warmupRuleDao.delete(ids);
            notifyWarmRulesChangedEvent();
        }
        catch (Exception e) {
            logger.error("failed to delete new rules ids=%s.", ids);
            throw new TrinoException(VaradaErrorCode.VARADA_RULE_CONFIGURATION_ERROR,
                    "failed to save new rules configuration",
                    e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private Map<SchemaTableName, Map<String, ColumnMetadata>> getTableColumnsMetadata(Collection<WarmupRule> warmupRules)
    {
        Set<SchemaTableName> schemaTableNames = warmupRules.stream()
                .map(this::getSchemaTableName)
                .collect(Collectors.toSet());
        ConnectorSession session = fakeConnectorSessionProvider.get();
        Pair<ConnectorMetadata, ConnectorTransactionHandle> proxiedMetadata = dispatcherProxiedConnectorTransformer.createProxiedMetadata(proxiedConnector, session);
        ConnectorMetadata metadata = proxiedMetadata.getKey();
        Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata = new HashMap<>();
        schemaTableNames.forEach(schemaTableName -> {
            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
                Map<String, ColumnMetadata> columnMetadataMap = columnHandles
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Entry::getKey, entry -> metadata.getColumnMetadata(session, tableHandle, entry.getValue())));
                tablesColumnsMetadata.put(schemaTableName, columnMetadataMap);
            }
        });
        proxiedConnector.commit(proxiedMetadata.getRight());
        return tablesColumnsMetadata;
    }

    private Map<String, ColumnMetadata> getTableColumnsMetadata(Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata, WarmupRule warmupRule)
    {
        SchemaTableName schemaTableName = getSchemaTableName(warmupRule);
        return tablesColumnsMetadata.get(schemaTableName);
    }

    private Optional<Type> getColumnType(Map<String, ColumnMetadata> columnsMetadata, WarmupRule warmupRule)
    {
        ColumnMetadata columnMetadata = columnsMetadata.get(warmupRule.getVaradaColumn().getName());
        if (columnMetadata != null) {
            return Optional.of(columnMetadata.getType());
        }
        String[] parts = warmupRule.getVaradaColumn().getName().split("#", -1);
        if (parts.length == 2) {
            columnMetadata = columnsMetadata.get(parts[0]);
            if (columnMetadata != null) {
                if (TypeUtils.isRowType(columnMetadata.getType())) {
                    RowType rowType = (RowType) columnMetadata.getType();
                    return rowType.getFields()
                            .stream()
                            .filter(field -> field.getName().equals(Optional.of(parts[1])))
                            .map(Field::getType)
                            .findAny();
                }
            }
        }
        return Optional.empty();
    }

    private SchemaTableName getSchemaTableName(WarmupRule warmupRule)
    {
        return new SchemaTableName(warmupRule.getSchema(), warmupRule.getTable());
    }

    public long getUniqueRuleId(WarmupRule warmupRule)
    {
        long ret = (2851L * warmupRule.getSchema().hashCode());
        ret += (2917L * warmupRule.getTable().hashCode());
        ret += (2999L * warmupRule.getVaradaColumn().hashCode());
        ret += (3061L * warmupRule.getWarmUpType().name().hashCode());
        if (warmupRule.getPredicates() != null) {
            long predicatesHash = warmupRule.getPredicates().stream()
                    .mapToLong(predicateRule -> Hashing.murmur3_128().hashInt(predicateRule.hashCode()).asLong())
                    .sum();
            ret += (3137 * predicatesHash);
        }
        return Hashing.murmur3_128().hashLong(ret).asLong();
    }

    private String getColumnUniqueKey(WarmupRule warmupRule)
    {
        int predicateHash = warmupRule.getPredicates().stream().mapToInt(WarmupPredicateRule::hashCode).sum();
        return String.format("%s:%s:%s:%s", warmupRule.getSchema(), warmupRule.getTable(), warmupRule.getVaradaColumn().getName(), predicateHash);
    }

    private void notifyWarmRulesChangedEvent()
    {
        eventBus.post(new WarmRulesChangedEvent());
    }
}
