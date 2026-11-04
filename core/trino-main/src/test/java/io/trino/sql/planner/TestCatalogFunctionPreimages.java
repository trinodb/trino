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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.DomainProjection;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.sql.ir.Call;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.IrUtils.preOrder;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCatalogFunctionPreimages
        extends BasePlanTest
{
    @BeforeAll
    public final void setup()
    {
        getPlanTester().installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withFunctions(ImmutableList.of(
                        catalogFunction("projected_identity")
                                .domainProjection(new DomainProjection((_, resultDomain) -> Optional.of(new PreimageResult(resultDomain, EXACT))))
                                .build(),
                        catalogFunction("opaque_identity").build()))
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
                    {
                        return ScalarFunctionImplementation.builder()
                                .methodHandle(MethodHandles.identity(long.class))
                                .build();
                    }
                }))
                .build()));
        getPlanTester().createCatalog("mock", "mock", ImmutableMap.of());
    }

    @Test
    public void testCatalogFunctionProjection()
    {
        assertThat(filterFunctions("SELECT nationkey FROM nation WHERE mock.system.projected_identity(nationkey) = 5"))
                .doesNotContain("projected_identity");
    }

    @Test
    public void testCatalogFunctionWithoutProjection()
    {
        assertThat(filterFunctions("SELECT nationkey FROM nation WHERE mock.system.opaque_identity(nationkey) = 5"))
                .contains("opaque_identity");
    }

    private List<String> filterFunctions(String sql)
    {
        return searchFrom(plan(sql).getRoot())
                .whereIsInstanceOfAny(FilterNode.class)
                .findAll().stream()
                .flatMap(filter -> preOrder(((FilterNode) filter).getPredicate()))
                .filter(Call.class::isInstance)
                .map(call -> ((Call) call).function().name().functionName())
                .toList();
    }

    private static FunctionMetadata.Builder catalogFunction(String name)
    {
        return FunctionMetadata.scalarBuilder(name)
                .signature(Signature.builder().argumentType(BIGINT).returnType(BIGINT).build())
                .description("")
                .neverFails();
    }
}
