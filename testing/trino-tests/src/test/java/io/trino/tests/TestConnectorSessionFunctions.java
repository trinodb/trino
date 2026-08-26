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
package io.trino.tests;

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.methodHandle;

public class TestConnectorSessionFunctions
        extends AbstractTestQueryFramework
{
    private static final MethodHandle MULTIPLY = methodHandle(TestConnectorSessionFunctions.class, "multiply", ConnectorSession.class, Object.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setAdditionalSetup(queryRunner -> {
                    queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                            .withFunctions(List.of(
                                    FunctionMetadata.scalarBuilder("multiply")
                                            .signature(Signature.builder()
                                                    .argumentType(BIGINT)
                                                    .returnType(BIGINT)
                                                    .build())
                                            .nullable()
                                            .argumentNullability(true)
                                            .hidden()
                                            .build(),
                                    FunctionMetadata.scalarBuilder("multiply_nonnull")
                                            .signature(Signature.builder()
                                                    .argumentType(BIGINT)
                                                    .returnType(BIGINT)
                                                    .build())
                                            .nullable()
                                            .hidden()
                                            .build()))
                            .withFunctionProvider(Optional.of(new FunctionProvider()
                            {
                                @Override
                                public ScalarFunctionImplementation getScalarFunctionImplementation(
                                        FunctionId functionId,
                                        BoundSignature boundSignature,
                                        FunctionDependencies functionDependencies,
                                        InvocationConvention invocationConvention)
                                {
                                    InvocationConvention actualConvention = new InvocationConvention(
                                            List.of(BOXED_NULLABLE),
                                            NULLABLE_RETURN,
                                            true,
                                            false);
                                    MethodHandle adapted = ScalarFunctionAdapter.adapt(
                                            MULTIPLY,
                                            boundSignature.getReturnType(),
                                            boundSignature.getArgumentTypes(),
                                            actualConvention,
                                            invocationConvention);
                                    return ScalarFunctionImplementation.builder()
                                            .methodHandle(adapted)
                                            .build();
                                }
                            }))
                            .build()));
                    queryRunner.createCatalog("mock", "mock");
                    queryRunner.installPlugin(new TpchPlugin());
                    queryRunner.createCatalog("tpch", "tpch");
                })
                .build();
    }

    @Test
    public void testNullableObjectReturnWithColumnArgument()
    {
        assertQuery("SELECT mock.default.multiply(nationkey) FROM tpch.tiny.nation WHERE nationkey = 1", "VALUES 2");
    }

    @Test
    public void testNullableObjectArgumentWithExpression()
    {
        assertQuery("SELECT mock.default.multiply(nationkey + length(name)) FROM tpch.tiny.nation WHERE nationkey = 1", "VALUES 20");
    }

    @Test
    public void testNullableObjectArgumentWithNonNullableExpression()
    {
        assertQuery("SELECT mock.default.multiply_nonnull(nationkey + length(name)) FROM tpch.tiny.nation WHERE nationkey = 1", "VALUES 20");
    }

    public static Object multiply(ConnectorSession session, Object value)
    {
        return value == null ? null : (Long) value * 2;
    }
}
