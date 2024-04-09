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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWindowNode
{
    private final TestingFunctionResolution functionResolution;
    private final ObjectMapper objectMapper;

    public TestWindowNode()
    {
        functionResolution = new TestingFunctionResolution();

        // dependencies copied from ServerMainModule.java to avoid depending on whole ServerMainModule here
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.of(
                Symbol.class, new SymbolKeyDeserializer(new TestingTypeManager()),
                TypeSignature.class, new TypeSignatureKeyDeserializer()));

        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(new TestingTypeManager()::getType)));

        objectMapper = provider.get();
    }

    @Test
    public void testSerializationRoundtrip()
            throws Exception
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol columnA = symbolAllocator.newSymbol("a", BIGINT);
        Symbol columnB = symbolAllocator.newSymbol("b", BIGINT);
        Symbol columnC = symbolAllocator.newSymbol("c", BIGINT);

        ValuesNode sourceNode = new ValuesNode(
                newId(),
                ImmutableList.of(columnA, columnB, columnC),
                ImmutableList.of());

        Symbol windowSymbol = symbolAllocator.newSymbol("sum", BIGINT);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction("sum", fromTypes(BIGINT));
        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        PlanNodeId id = newId();
        DataOrganizationSpecification specification = new DataOrganizationSpecification(
                ImmutableList.of(columnA),
                Optional.of(new OrderingScheme(
                        ImmutableList.of(columnB),
                        ImmutableMap.of(columnB, SortOrder.ASC_NULLS_FIRST))));
        Map<Symbol, WindowNode.Function> functions = ImmutableMap.of(windowSymbol, new WindowNode.Function(resolvedFunction, ImmutableList.of(columnC.toSymbolReference()), frame, false));
        Optional<Symbol> hashSymbol = Optional.of(columnB);
        Set<Symbol> prePartitionedInputs = ImmutableSet.of(columnA);
        WindowNode windowNode = new WindowNode(
                id,
                sourceNode,
                specification,
                functions,
                hashSymbol,
                prePartitionedInputs,
                0);

        String json = objectMapper.writeValueAsString(windowNode);

        WindowNode actualNode = objectMapper.readValue(json, WindowNode.class);

        assertThat(actualNode.getId()).isEqualTo(windowNode.getId());
        assertThat(actualNode.getSpecification()).isEqualTo(windowNode.getSpecification());
        assertThat(actualNode.getWindowFunctions()).isEqualTo(windowNode.getWindowFunctions());
        assertThat(actualNode.getFrames()).isEqualTo(windowNode.getFrames());
        assertThat(actualNode.getHashSymbol()).isEqualTo(windowNode.getHashSymbol());
        assertThat(actualNode.getPrePartitionedInputs()).isEqualTo(windowNode.getPrePartitionedInputs());
        assertThat(actualNode.getPreSortedOrderPrefix()).isEqualTo(windowNode.getPreSortedOrderPrefix());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
