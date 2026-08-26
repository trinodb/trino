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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.trino.testing.AbstractTestEngineOnlyQueries;
import io.trino.testing.CustomFunctionBundle;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.AbstractTestEngineOnlyQueries.TESTING_CATALOG;
import static io.trino.util.Reflection.methodHandle;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestLocalEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    private static final MethodHandle MULTIPLY = methodHandle(TestLocalEngineOnlyQueries.class, "multiply", ConnectorSession.class, long.class);

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = TestLocalQueries.createTestQueryRunner();
        try {
            queryRunner.addFunctions(CustomFunctionBundle.CUSTOM_FUNCTIONS);
            // for testing session properties
            queryRunner.getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .withFunctions(ImmutableList.of(FunctionMetadata.scalarBuilder("multiply")
                            .signature(Signature.builder()
                                    .argumentType(BIGINT)
                                    .returnType(BIGINT)
                                    .build())
                            .description("")
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
            queryRunner.createCatalog(TESTING_CATALOG, "mock", ImmutableMap.of());
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    public void testConnectorFunctionLiteralArgumentUsesCatalogSession()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(TESTING_CATALOG, "connector_long", "3")
                .build();

        assertQuery(session, "SELECT testing_catalog.default.multiply(5)", "SELECT CAST(15 AS BIGINT)");
    }

    public static long multiply(ConnectorSession session, long value)
    {
        return session.getProperty("connector_long", Long.class) * value;
    }

    @Test
    @Override
    public void testSetSession()
    {
        abort("SET SESSION is not supported by PlanTester");
    }

    @Test
    @Override
    public void testResetSession()
    {
        abort("RESET SESSION is not supported by PlanTester");
    }
}
