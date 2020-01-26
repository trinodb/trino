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
package io.prestosql.tests;

import io.prestosql.Session;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestExecutionUserSessionProperty
{
    private DistributedQueryRunner queryRunner;
    private Optional<Principal> principal;
    private final Optional<Principal> impersonatingService = Optional.of(new BasicPrincipal("impersonatingService"));
    private final Optional<Principal> user = Optional.of(new BasicPrincipal("user"));

    @BeforeClass
    private void setUp() throws Exception
    {
        Session session = testSessionBuilder().build();
        queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
        queryRunner.getAccessControl().setTestingSystemAccessControl(new TestingExecutionUserSessionProperty());
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testImpersonationSettingExecutionUser()
    {
        principal = impersonatingService;
        queryRunner.execute("SET SESSION execution_user = 'headless_account1'");
        try {
            queryRunner.execute("SET SESSION execution_user = 'headless_account2'");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Access Denied: Principal impersonatingService cannot become user headless_account2");
        }
    }

    @Test
    public void testImpersonationSecurityIssue()
    {
        principal = impersonatingService;
        try {
            queryRunner.execute("SET SESSION execution_user = 'user2'");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Access Denied: Principal user cannot become user user2");
        }
    }

    @Test
    public void testSelfSettingExecutionUser()
    {
        principal = user;
        MaterializedResult materializedResult = queryRunner.execute("SET SESSION execution_user = 'headless_account1'");
        assertEquals(materializedResult.getSetSessionProperties().get("execution_user"), "headless_account1");
        materializedResult = queryRunner.execute("SET SESSION execution_user = 'user'");
        assertEquals(materializedResult.getSetSessionProperties().get("execution_user"), "user");
        try {
            queryRunner.execute("SET SESSION execution_user = 'user2'");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Access Denied: Principal user cannot become user user2");
        }

        try {
            queryRunner.execute("SET SESSION execution_user = reverse('abc')");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Access Denied: Principal user cannot become user cba");
        }

        materializedResult = queryRunner.execute("SET SESSION execution_user = reverse('resu')");
        assertEquals(materializedResult.getSetSessionProperties().get("execution_user"), "user");
    }

    private class TestingExecutionUserSessionProperty
            implements SystemAccessControl
    {
        private final Map<String, Set<String>> canBeSetMapping = new HashMap<>();

        public TestingExecutionUserSessionProperty()
        {
            canBeSetMapping.put("user", new HashSet<>(Arrays.asList("user", "headless_account1")));
            canBeSetMapping.put("impersonatingService", new HashSet<>(Arrays.asList("user", "user2", "headless_account1")));
        }

        @Override
        public void checkCanSetUser(Optional<Principal> principal, String userName)
        {
            if (!principal.isPresent()) {
                principal = TestExecutionUserSessionProperty.this.principal;
            }
            Set<String> userNames = canBeSetMapping.get(principal.get().getName());
            if (userNames == null || userNames.isEmpty()) {
                AccessDeniedException.denySetUser(principal, userName);
            }

            if (userNames.contains(userName)) {
                return;
            }
            AccessDeniedException.denySetUser(principal, userName);
        }

        @Override
        public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
        {
        }
    }
}
