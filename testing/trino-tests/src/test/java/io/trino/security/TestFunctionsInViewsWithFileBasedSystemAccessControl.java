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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.security.Identity;
import io.trino.sql.SqlPath;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestFunctionsInViewsWithFileBasedSystemAccessControl
        extends AbstractTestQueryFramework
{
    public static final Session ALICE_USER = user("alice"); // can create views && can query views && can execute and grant execute functions
    public static final Session BOB_USER = user("bob"); // can create views && can query views && can execute functions
    public static final Session CHARLIE_USER = user("charlie"); // can query views

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String securityConfigFile = new File(getResource("file-based-system-functions-access.json").toURI()).getPath();
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(Optional.empty())
                        .setSchema(Optional.empty())
                        .setPath(SqlPath.buildPath("mock.function", Optional.empty()))
                        .build())
                .setWorkerCount(0)
                .setSystemAccessControl("file", Map.of(SECURITY_CONFIG_FILE, securityConfigFile))
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new TestingTableFunctions.SimpleTableFunction()))
                .withApplyTableFunction((session, handle) -> {
                    if (handle instanceof TestingTableFunctions.SimpleTableFunction.SimpleTableFunctionHandle functionHandle) {
                        return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow()));
                    }
                    throw new IllegalStateException("Unsupported table function handle: " + handle.getClass().getSimpleName());
                })
                .withFunctions(ImmutableList.<FunctionMetadata>builder()
                        .add(FunctionMetadata.scalarBuilder("my_function")
                                .signature(Signature.builder().returnType(BIGINT).build())
                                .noDescription()
                                .build())
                        .build())
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies, InvocationConvention invocationConvention)
                    {
                        return ScalarFunctionImplementation.builder()
                                .methodHandle(MethodHandles.constant(long.class, 42L))
                                .build();
                    }
                }))
                .build()));
        queryRunner.createCatalog("mock", "mock");
        return queryRunner;
    }

    @Test
    public void testPtfSecurityDefinerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_ptf_alice_security_definer SECURITY DEFINER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_ptf_alice_security_definer";
        assertQuerySucceeds(ALICE_USER, securityDefinerQuery);
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertQuerySucceeds(CHARLIE_USER, securityDefinerQuery);
    }

    @Test
    public void testPtfSecurityInvokerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_ptf_alice_security_invoker SECURITY INVOKER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_ptf_alice_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function mock.system.simple_table_function");
    }

    @Test
    public void testFunctionSecurityDefinerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_function_alice_security_definer SECURITY DEFINER AS SELECT my_function() AS t");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_function_alice_security_definer";
        assertQuerySucceeds(ALICE_USER, securityDefinerQuery);
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertQuerySucceeds(CHARLIE_USER, securityDefinerQuery);
    }

    @Test
    public void testFunctionSecurityInvokerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_function_alice_security_invoker SECURITY INVOKER AS SELECT my_function() AS t");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_function_alice_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function my_function");
    }

    @Test
    public void testPtfSecurityDefinerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_ptf_bob_security_definer SECURITY DEFINER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_ptf_bob_security_definer";
        assertAccessDenied(ALICE_USER, securityDefinerQuery, "Cannot execute function mock.system.simple_table_function");
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertAccessDenied(CHARLIE_USER, securityDefinerQuery, "Cannot execute function mock.system.simple_table_function");
    }

    @Test
    public void testPtfSecurityInvokerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_ptf_bob_security_invoker SECURITY INVOKER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_ptf_bob_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function mock.system.simple_table_function");
    }

    @Test
    public void testFunctionSecurityDefinerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_function_bob_security_definer SECURITY DEFINER AS SELECT my_function() AS t");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_function_bob_security_definer";
        assertAccessDenied(ALICE_USER, securityDefinerQuery, "Cannot execute function my_function");
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertAccessDenied(CHARLIE_USER, securityDefinerQuery, "Cannot execute function my_function");
    }

    @Test
    public void testFunctionSecurityInvokerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_function_bob_security_invoker SECURITY INVOKER AS SELECT my_function() AS t");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_function_bob_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function my_function");
    }

    private static Session user(String user)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(user))
                .setPath(SqlPath.buildPath("mock.function", Optional.empty()))
                .build();
    }
}
