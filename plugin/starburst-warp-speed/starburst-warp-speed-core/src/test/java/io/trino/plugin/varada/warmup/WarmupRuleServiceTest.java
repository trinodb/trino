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

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.di.DefaultFakeConnectorSessionProvider;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.warmup.dal.WarmupRuleDao;
import io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.varada.warmup.model.WarmupRuleResult;
import io.trino.plugin.warp.gen.constants.WarmUpType;
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
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.JsonType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WarmupRuleServiceTest
{
    public static final String COL1 = "col1";

    private WarmupRuleService warmupRuleService;
    private Map<String, ColumnHandle> columnMap;
    private ConnectorTableHandle tableHandle;
    private ConnectorMetadata connectorMetadata;
    private StorageEngineConstants storageEngineConstants;
    private WarmupRuleDao warmupRuleDao;
    private GlobalConfiguration globalConfiguration;

    @BeforeEach
    public void before()
    {
        columnMap = new HashMap<>();
        warmupRuleDao = mock(WarmupRuleDao.class);

        this.storageEngineConstants = spy(new StubsStorageEngineConstants(1000));
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        this.connectorMetadata = mock(ConnectorMetadata.class);
        ConnectorTransactionHandle connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        Connector proxiedConnector = mock(Connector.class);
        when(dispatcherProxiedConnectorTransformer.createProxiedMetadata(eq(proxiedConnector), any(ConnectorSession.class)))
                .thenReturn(Pair.of(connectorMetadata, connectorTransactionHandle));
        this.tableHandle = mock(ConnectorTableHandle.class);
        when(connectorMetadata.getTableHandle(any(ConnectorSession.class), any(SchemaTableName.class), eq(Optional.empty()), eq(Optional.empty()))).thenAnswer(inv -> tableHandle);
        when(connectorMetadata.getColumnHandles(any(ConnectorSession.class), eq(tableHandle))).thenAnswer((inv) -> columnMap);
        this.globalConfiguration = new GlobalConfiguration();
        warmupRuleService = new WarmupRuleService(proxiedConnector,
                storageEngineConstants,
                new WarmupDemoterConfiguration(),
                dispatcherProxiedConnectorTransformer,
                new DefaultFakeConnectorSessionProvider(),
                warmupRuleDao,
                mock(EventBus.class),
                new VaradaInitializedServiceRegistry(),
                globalConfiguration);
    }

    @Test
    public void testSimpleCRUD()
    {
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        warmupRuleService.save(List.of(warmupRule));

        when(warmupRuleDao.getAll()).thenReturn(List.of(warmupRule));
        Collection<WarmupRule> allRules = warmupRuleService.getAll();
        assertThat(allRules).containsExactly(warmupRule);

        warmupRuleService.delete(allRules.stream().map(WarmupRule::getId).collect(Collectors.toList()));

        when(warmupRuleDao.getAll()).thenReturn(List.of());
        assertThat(warmupRuleService.getAll().size()).isEqualTo(0);
    }

    @Test
    public void testRejectIndexRulesWhenDataOnlyFlagIsOn()
    {
        globalConfiguration.setDataOnlyWarming(true);
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules()).isEmpty();
        assertThat(warmupRuleResult.rejectedRules()).isNotEmpty();
    }

    @Test
    public void testSameUniqueConstraint()
    {
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule1 = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        WarmupRule warmupRule2 = WarmupRule.builder(warmupRule1)/*.id(0)*/.build();

        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule1, warmupRule2));
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                .contains(Integer.toString(VaradaErrorCode.VARADA_DUPLICATE_RECORD.getCode()))).isTrue();
    }

    @Test
    public void testWarmupRuleId()
    {
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRuleNew1 = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        WarmupRule warmupRuleSaved1 = WarmupRule.builder(warmupRuleNew1).id(1).build();

        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRuleNew1));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.appliedRules().get(0).getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_LUCENE);
        when(warmupRuleDao.getAll()).thenReturn(List.of(warmupRuleSaved1));

        WarmupRule warmupRule2 = WarmupRule.builder(createRule(WarmUpType.WARM_UP_TYPE_DATA)).id(1).build();
        warmupRuleResult = warmupRuleService.save(List.of(warmupRule2));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.appliedRules().get(0).getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_DATA);
        when(warmupRuleDao.getAll()).thenReturn(List.of(warmupRule2));

        WarmupRule warmupRule3 = WarmupRule.builder(createRule(WarmUpType.WARM_UP_TYPE_BASIC)).id(2).build();
        warmupRuleResult = warmupRuleService.save(List.of(warmupRule3));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(0);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().keySet().stream().findAny().orElseThrow().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_BASIC);
        assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow().contains(Integer.toString(VaradaErrorCode.VARADA_WARMUP_RULE_ID_NOT_VALID.getCode()))).isTrue();
    }

    @Test
    public void testWarmupTypeDoesntSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(RowType.rowType(RowType.field(baseType)), "doesn't support column type row",
                new ArrayType(baseType), "doesn't support column type array",
                new MapType(baseType, baseType, new TypeOperators()), "doesn't support column type map",
                JsonType.JSON, "doesn't support column type json");
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_BASIC);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(VaradaErrorCode.VARADA_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testLuceneWarmupTypeDoesntSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(RowType.rowType(RowType.field(baseType)), "doesn't support column type row",
                new MapType(baseType, baseType, new TypeOperators()), "doesn't support column type map",
                JsonType.JSON, "doesn't support column type json");
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(VaradaErrorCode.VARADA_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testLuceneWarmupTypeSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);

        Set<Type> supportedTypeToMessage = Set.of(new ArrayType(baseType));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        supportedTypeToMessage.forEach((type) -> {
            createColumn(type);
            assertRuleApplied(warmupRule);
        });
    }

    @Test
    public void testWarmupTypeDoesntSupportDataColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(new ArrayType(RowType.rowType(RowType.field(baseType))), "doesn't support column type array");

        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(VaradaErrorCode.VARADA_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testNeverOnUnsupportedColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        List<Type> unsupportedTypes = ImmutableList.of(RowType.rowType(RowType.field(baseType)),
                new ArrayType(baseType),
                new MapType(baseType, baseType, new TypeOperators()),
                JsonType.JSON);
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_BASIC);

        for (int i = 0, unsupportedTypesSize = unsupportedTypes.size(); i < unsupportedTypesSize; i++) {
            Type type = unsupportedTypes.get(i);
            String columnName = String.valueOf(i);
            createColumn(columnName, type);
            WarmupRule neverRule = WarmupRule.builder(warmupRule).varadaColumn(new RegularColumn(columnName)).priority(-10).build();
            assertRuleApplied(neverRule);
        }
    }

    @Test
    public void testBloomWithIllegalIndexLengthShouldFail()
    {
        createColumn(SmallintType.SMALLINT);
        assertRuleRejected(createRule(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM),
                "Bloom index can be applied only to types that use index length");
    }

    @Test
    public void testUnknownTable()
    {
        tableHandle = null;
        assertRuleRejected(createRule(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM),
                "Rule refer to a non-exist table");
    }

    @Test
    public void testUnknownColumn()
    {
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE,
                Set.of(new PartitionValueWarmupPredicateRule("col2", "2")));
        assertRuleRejected(warmupRule, "Rule refer to a non-exist column");
    }

    @Test
    public void testMultipleBloomShouldFail()
    {
        createColumn(IntegerType.INTEGER);
        WarmupRule warmupRule1 = createRule(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM, Collections.emptySet());

        WarmupRule warmupRule2 = WarmupRule.builder(warmupRule1).warmUpType(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH).build();
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule1, warmupRule2));
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                .contains("Only single bloom index allowed")).isTrue();
    }

    @Test
    public void testLongCharShouldFail()
    {
        when(storageEngineConstants.getMaxRecLen()).thenReturn(8);
        createColumn(CharType.createCharType(255));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA, Collections.emptySet());
        assertRuleRejected(warmupRule, "Can't warm long Char columns");
    }

    @Test
    public void testCharRule()
    {
        when(storageEngineConstants.getMaxRecLen()).thenReturn(8);
        createColumn(CharType.createCharType(7));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA, Collections.emptySet());
        assertRuleApplied(warmupRule);
    }

    @Test
    public void testUniquenessBetweenSaves()
    {
        createColumn(VarcharType.createVarcharType(10));

        WarmupRule warmupRule1 = WarmupRule.builder()
                .schema("bundle")
                .table("bundle")
                .varadaColumn(new RegularColumn(COL1))
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .priority(0)
                .ttl(0)
                .predicates(Set.of())
                .build();

        warmupRuleService.save(List.of(warmupRule1));

        try {
            warmupRuleService.save(List.of(warmupRule1));
        }
        catch (TrinoException e) {
            assertThat(e.getErrorCode()).isEqualTo(VaradaErrorCode.VARADA_DUPLICATE_RECORD.toErrorCode());
        }
    }

    public WarmupRule createRule(WarmUpType warmUpType)
    {
        VaradaColumn defaultVaradaColumn = new RegularColumn(COL1);
        Set<WarmupPredicateRule> defaultPredicates = Set.of(new PartitionValueWarmupPredicateRule(COL1, "2"));
        return createRule(warmUpType, defaultVaradaColumn, defaultPredicates);
    }

    public WarmupRule createRule(WarmUpType warmUpType, Set<WarmupPredicateRule> predicates)
    {
        VaradaColumn defaultVaradaColumn = new RegularColumn(COL1);
        return createRule(warmUpType, defaultVaradaColumn, predicates);
    }

    private WarmupRule createRule(WarmUpType warmUpType, VaradaColumn varadaColumn, Set<WarmupPredicateRule> predicates)
    {
        return WarmupRule.builder()
                .schema("schema")
                .table("table")
                .varadaColumn(varadaColumn)
                .warmUpType(warmUpType)
                .priority(0)
                .ttl(0)
                //.id(0)
                .predicates(predicates)
                .build();
    }

    private void createColumn(Type type)
    {
        createColumn(COL1, type);
    }

    private void createColumn(String name, Type type)
    {
        ColumnHandle columnHandle = mock(ColumnHandle.class);
        columnMap.put(name, columnHandle);
        ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
        when(columnMetadata.getName()).thenReturn(name);
        when(columnMetadata.getType()).thenReturn(type);
        when(connectorMetadata.getColumnMetadata(any(ConnectorSession.class), eq(tableHandle), eq(columnHandle))).thenReturn(columnMetadata);
    }

    private void assertRuleApplied(WarmupRule warmupRule)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(ImmutableList.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(0);
    }

    private void assertRuleRejected(WarmupRule warmupRule, String... expectedRejects)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(ImmutableList.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(0);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        Optional<Map.Entry<WarmupRule, Set<String>>> rejectedRule = warmupRuleResult.rejectedRules()
                .entrySet()
                .stream()
                .findFirst();
        assertThat(rejectedRule.isPresent()).isTrue();
        Set<String> actualRejects = rejectedRule.orElseThrow().getValue();
        assertThat(actualRejects.size()).isEqualTo(expectedRejects.length);
        Stream.of(expectedRejects)
                .forEach(expected -> assertThat(actualRejects.stream().anyMatch(actual -> actual.contains(expected))).isTrue());
    }
}
