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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
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
        String securityConfigFile = getResource("file-based-system-functions-access.json").getPath();
        SystemAccessControl accessControl = new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, securityConfigFile));
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(Optional.empty())
                        .setSchema(Optional.empty())
                        .build())
                .setNodeCount(1)
                .setSystemAccessControl(accessControl)
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
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot access catalog mock");
    }

    @Test
    public void testFunctionSecurityDefinerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_function_alice_security_definer SECURITY DEFINER AS SELECT now() AS t");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_function_alice_security_definer";
        assertQuerySucceeds(ALICE_USER, securityDefinerQuery);
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertQuerySucceeds(CHARLIE_USER, securityDefinerQuery);
    }

    @Test
    public void testFunctionSecurityInvokerViewCreatedByAlice()
    {
        assertQuerySucceeds(ALICE_USER, "CREATE VIEW blackhole.default.view_function_alice_security_invoker SECURITY INVOKER AS SELECT now() AS t");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_function_alice_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function now");
    }

    @Test
    public void testPtfSecurityDefinerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_ptf_bob_security_definer SECURITY DEFINER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_ptf_bob_security_definer";
        assertAccessDenied(ALICE_USER, securityDefinerQuery, "View owner does not have sufficient privileges: 'bob' cannot grant 'mock\\.system\\.simple_table_function' execution to user 'alice'");
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertAccessDenied(CHARLIE_USER, securityDefinerQuery, "View owner does not have sufficient privileges: 'bob' cannot grant 'mock\\.system\\.simple_table_function' execution to user 'charlie'");
    }

    @Test
    public void testPtfSecurityInvokerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_ptf_bob_security_invoker SECURITY INVOKER AS SELECT * FROM TABLE(mock.system.simple_table_function())");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_ptf_bob_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot access catalog mock");
    }

    @Test
    public void testFunctionSecurityDefinerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_function_bob_security_definer SECURITY DEFINER AS SELECT now() AS t");
        String securityDefinerQuery = "SELECT * FROM blackhole.default.view_function_bob_security_definer";
        assertAccessDenied(ALICE_USER, securityDefinerQuery, "View owner does not have sufficient privileges: 'bob' cannot grant 'now' execution to user 'alice'");
        assertQuerySucceeds(BOB_USER, securityDefinerQuery);
        assertAccessDenied(CHARLIE_USER, securityDefinerQuery, "View owner does not have sufficient privileges: 'bob' cannot grant 'now' execution to user 'charlie'");
    }

    @Test
    public void testFunctionSecurityInvokerViewCreatedByBob()
    {
        assertQuerySucceeds(BOB_USER, "CREATE VIEW blackhole.default.view_function_bob_security_invoker SECURITY INVOKER AS SELECT now() AS t");
        String securityInvokerQuery = "SELECT * FROM blackhole.default.view_function_bob_security_invoker";
        assertQuerySucceeds(ALICE_USER, securityInvokerQuery);
        assertQuerySucceeds(BOB_USER, securityInvokerQuery);
        assertAccessDenied(CHARLIE_USER, securityInvokerQuery, "Cannot execute function now");
    }

    private static Session user(String user)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
