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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * LDAP authentication tests using CLI.
 * <p>
 * These tests run the Trino CLI inside the container using docker exec.
 */
@ProductTest
@RequiresEnvironment(LdapBasicEnvironment.class)
@TestGroup.Ldap
@TestGroup.LdapCli
@TestGroup.ProfileSpecificTests
class TestTrinoLdapCli
{
    private static final String SELECT_FROM_NATION = "SELECT count(*) FROM tpch.tiny.nation";

    @Test
    void shouldRunQueryWithLdap(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                env.getDefaultUser(),
                env.getDefaultPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldRunBatchQueryWithLdap(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                env.getDefaultUser(),
                env.getDefaultPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldPassQueryForLdapUserInMultipleGroups(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                env.getUserInMultipleGroups(),
                env.getLdapPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    @TestGroup.LdapMultipleBinds
    void shouldPassQueryForAlternativeLdapBind(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                env.getUserInAmerica(),
                env.getLdapPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldFailQueryForLdapUserInChildGroup(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli(env.getChildGroupUser(), env.getLdapPassword(), "--execute", SELECT_FROM_NATION),
                format("User [%s] not a member of an authorized group", env.getChildGroupUser()));
    }

    @Test
    void shouldFailQueryForLdapUserInParentGroup(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli(env.getParentGroupUser(), env.getLdapPassword(), "--execute", SELECT_FROM_NATION),
                format("User [%s] not a member of an authorized group", env.getParentGroupUser()));
    }

    @Test
    void shouldFailQueryForOrphanLdapUser(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli(env.getOrphanUser(), env.getLdapPassword(), "--execute", SELECT_FROM_NATION),
                format("User [%s] not a member of an authorized group", env.getOrphanUser()));
    }

    @Test
    @TestGroup.LdapMultipleBinds
    void shouldFailQueryForFailedBind(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli(env.getUserInEurope(), env.getLdapPassword(), "--execute", SELECT_FROM_NATION),
                env.expectedFailedBindMessage());
    }

    @Test
    void shouldFailQueryForWrongLdapPassword(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli(env.getDefaultUser(), "wrong_password", "--execute", SELECT_FROM_NATION),
                env.expectedWrongLdapPasswordMessage());
    }

    @Test
    void shouldFailQueryForWrongLdapUser(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCli("invalid_user", env.getLdapPassword(), "--execute", SELECT_FROM_NATION),
                env.expectedWrongLdapUserMessage());
    }

    @Test
    void shouldFailQueryForEmptyUser(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                "",
                env.getLdapPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isNotZero();
        assertThat(result.getStderr()).contains("Both username and password must be specified");
    }

    @Test
    void shouldFailQueryForLdapWithoutPassword(LdapBasicEnvironment env)
            throws Exception
    {
        assertCliAuthenticationFailure(
                env.executeCliWithoutPassword(env.getDefaultUser(), "--execute", SELECT_FROM_NATION),
                "Authentication failed: Unauthorized");
    }

    @Test
    void shouldPassForCredentialsWithSpecialCharacters(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                env.getSpecialUser(),
                env.getSpecialUserPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldFailForUserWithColon(LdapBasicEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli(
                "UserWith:Colon",
                env.getLdapPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isNotZero();
        assertThat(result.getStderr()).contains("Illegal character ':' found in username");
    }

    @Test
    void shouldRunQueryFromFileWithLdap(LdapBasicEnvironment env)
            throws Exception
    {
        // Create a temporary SQL file inside the container
        ExecResult createFile = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "echo 'SELECT count(*) FROM tpch.tiny.nation;' > /tmp/test_query.sql");
        assertThat(createFile.getExitCode()).isZero();

        ExecResult result = env.executeCli(
                env.getDefaultUser(),
                env.getDefaultPassword(),
                "--file", "/tmp/test_query.sql");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    private void assertCliAuthenticationFailure(ExecResult result, String expectedMessage)
    {
        assertThat(result.getExitCode()).isNotZero();
        assertThat(result.getStderr()).contains(expectedMessage);
    }

    @Test
    void shouldFailQueryForLdapWithoutHttps(LdapBasicEnvironment env)
            throws Exception
    {
        // Test that HTTP (non-HTTPS) connections are rejected
        ExecResult result = env.executeCliWithHttpUrl(
                env.getDefaultUser(),
                env.getDefaultPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isNotZero();
        assertThat(result.getStderr()).containsAnyOf(
                "Authentication using username/password requires HTTPS to be enabled",
                "TLS/SSL is required for authentication with username and password");
    }

    @Test
    void shouldFailForIncorrectTrustStore(LdapBasicEnvironment env)
            throws Exception
    {
        // Test with incorrect truststore password
        ExecResult result = env.executeCliWithWrongTruststorePassword(
                env.getDefaultUser(),
                env.getDefaultPassword(),
                "--execute", SELECT_FROM_NATION);

        assertThat(result.getExitCode()).isNotZero();
        assertThat(result.getStderr()).contains("keystore password was incorrect");
    }
}
