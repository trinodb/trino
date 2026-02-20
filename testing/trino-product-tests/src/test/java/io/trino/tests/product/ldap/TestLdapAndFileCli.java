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
package io.trino.tests.product.ldap;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LDAP and file-based authentication tests using CLI.
 * <p>
 * Tests scenarios where both LDAP and file password authenticators are configured.
 */
@ProductTest
@RequiresEnvironment(LdapAndFileEnvironment.class)
@TestGroup.LdapAndFile
@TestGroup.LdapAndFileCli
@TestGroup.ProfileSpecificTests
class TestLdapAndFileCli
{
    private static final String SELECT_FROM_NATION = "SELECT count(*) FROM tpch.tiny.nation";

    @Test
    void shouldRunQueryWithLdapAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use LDAP password to authenticate
        ExecResult result = env.executeCli(
                env.getDefaultUser(),
                env.getLdapPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldRunQueryWithFileAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use file-based password to authenticate (same user, different password)
        ExecResult result = env.executeCli(
                env.getDefaultUser(),
                env.getFileUserPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldRunQueryForFileOnlyUser(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use file-only user (not in LDAP)
        ExecResult result = env.executeCli(
                env.getOnlyFileUser(),
                env.getOnlyFileUserPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldRunQueryForAnotherUserWithOnlyFileAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Legacy parity alias for file-only user authentication coverage.
        shouldRunQueryForFileOnlyUser(env);
    }
}
