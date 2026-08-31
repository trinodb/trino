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

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.methodHandle;

public class TestConnectorSessionFunctions
        extends AbstractTestQueryFramework
{
    private static final MethodHandle MULTIPLY = methodHandle(TestConnectorSessionFunctions.class, "multiply", ConnectorSession.class, long.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setAdditionalSetup(queryRunner -> {
                    queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                            .withSessionProperty(longProperty("connector_long", "Multiplier", 2L, false))
                            .withFunctions(List.of(FunctionMetadata.scalarBuilder("multiply")
                                    .signature(Signature.builder()
                                            .argumentType(BIGINT)
                                            .returnType(BIGINT)
                                            .build())
                                    .noDescription()
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
                                    return ScalarFunctionImplementation.builder()
                                            .methodHandle(MULTIPLY)
                                            .build();
                                }
                            }))
                            .build()));
                    queryRunner.createCatalog("mock", "mock");
                })
                .build();
    }

    @Test
    public void testConnectorFunctionLiteralArgumentUsesCatalogSession()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("mock", "connector_long", "3")
                .build();

        assertQuery(session, "SELECT mock.default.multiply(5)", "VALUES 15");
    }

    public static long multiply(ConnectorSession session, long value)
    {
        return session.getProperty("connector_long", Long.class) * value;
    }
}
