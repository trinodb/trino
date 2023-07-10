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
import io.trino.server.ExpressionSerialization.ExpressionDeserializer;
import io.trino.server.ExpressionSerialization.ExpressionSerializer;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WindowFrame;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestWindowNode
{
    private final TestingFunctionResolution functionResolution;
    private SymbolAllocator symbolAllocator;
    private ValuesNode sourceNode;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;

    private final ObjectMapper objectMapper;

    public TestWindowNode()
    {
        functionResolution = new TestingFunctionResolution();

        // dependencies copied from ServerMainModule.java to avoid depending on whole ServerMainModule here
        SqlParser sqlParser = new SqlParser();
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Expression.class, new ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(functionResolution.getPlannerContext().getTypeManager()),
                Expression.class, new ExpressionDeserializer(sqlParser),
                TypeSignature.class, new TypeSignatureDeserializer()));
        provider.setKeyDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureKeyDeserializer()));
        objectMapper = provider.get();
    }

    @BeforeClass
    public void setUp()
    {
        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", BIGINT);
        columnC = symbolAllocator.newSymbol("c", BIGINT);

        sourceNode = new ValuesNode(
                newId(),
                ImmutableList.of(columnA, columnB, columnC),
                ImmutableList.of());
    }

    @Test
    public void testSerializationRoundtrip()
            throws Exception
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", BIGINT);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
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

        assertEquals(actualNode.getId(), windowNode.getId());
        assertEquals(actualNode.getSpecification(), windowNode.getSpecification());
        assertEquals(actualNode.getWindowFunctions(), windowNode.getWindowFunctions());
        assertEquals(actualNode.getFrames(), windowNode.getFrames());
        assertEquals(actualNode.getHashSymbol(), windowNode.getHashSymbol());
        assertEquals(actualNode.getPrePartitionedInputs(), windowNode.getPrePartitionedInputs());
        assertEquals(actualNode.getPreSortedOrderPrefix(), windowNode.getPreSortedOrderPrefix());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
