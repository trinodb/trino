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
package io.trino.tests.product.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.ldap.LdapObjectRequirement;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.process.CliProcess.trimLines;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.AMERICA_ORG;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.ASIA_ORG;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.CHILD_GROUP;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.CHILD_GROUP_USER;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.DEFAULT_GROUP;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.DEFAULT_GROUP_USER;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.EUROPE_ORG;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.ORPHAN_USER;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.PARENT_GROUP;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.PARENT_GROUP_USER;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.SPECIAL_USER;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.USER_IN_AMERICA;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.USER_IN_EUROPE;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.USER_IN_MULTIPLE_GROUPS;
import static io.trino.tests.product.TestGroups.LDAP;
import static io.trino.tests.product.TestGroups.LDAP_AND_FILE_CLI;
import static io.trino.tests.product.TestGroups.LDAP_CLI;
import static io.trino.tests.product.TestGroups.LDAP_MULTIPLE_BINDS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoLdapCli
        extends TrinoCliLauncher
        implements RequirementsProvider
{
    private static final String SELECT_FROM_NATION = "SELECT * FROM hive.default.nation;";

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_path")
    private String ldapTruststorePath;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_password")
    private String ldapTruststorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_name")
    private String ldapUserName;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_server_address")
    private String ldapServerAddress;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_password")
    private String ldapUserPassword;

    @Inject(optional = true)
    @Named("databases.presto.file_user_password")
    private String fileUserPassword;

    @Inject(optional = true)
    @Named("databases.OnlyFileUser@presto.file_user_password")
    private String onlyFileUserPassword;

    public TestTrinoLdapCli()
            throws IOException
    {}

    @AfterTestWithContext
    @Override
    public void stopPresto()
            throws InterruptedException
    {
        super.stopPresto();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(new LdapObjectRequirement(
                        Arrays.asList(
                                AMERICA_ORG, ASIA_ORG, EUROPE_ORG,
                                DEFAULT_GROUP, PARENT_GROUP, CHILD_GROUP,
                                DEFAULT_GROUP_USER, PARENT_GROUP_USER, CHILD_GROUP_USER, ORPHAN_USER, SPECIAL_USER, USER_IN_MULTIPLE_GROUPS, USER_IN_AMERICA, USER_IN_EUROPE)),
                immutableTable(NATION));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryWithLdap()
            throws IOException
    {
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();
        trino.getProcessInput().println(SELECT_FROM_NATION);
        assertThat(trimLines(trino.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunBatchQueryWithLdap()
            throws IOException
    {
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryFromFileWithLdap()
            throws IOException
    {
        File temporayFile = File.createTempFile("test-sql", null);
        temporayFile.deleteOnExit();
        Files.write(SELECT_FROM_NATION + "\n", temporayFile, UTF_8);

        launchTrinoCliWithServerArgument("--file", temporayFile.getAbsolutePath());
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassQueryForLdapUserInMultipleGroups()
            throws IOException
    {
        ldapUserName = USER_IN_MULTIPLE_GROUPS.getAttributes().get("cn");

        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, LDAP_MULTIPLE_BINDS, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassQueryForAlternativeLdapBind()
            throws IOException
    {
        ldapUserName = USER_IN_AMERICA.getAttributes().get("cn");

        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInChildGroup()
            throws IOException
    {
        ldapUserName = CHILD_GROUP_USER.getAttributes().get("cn");
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains(format("User [%s] not a member of an authorized group", ldapUserName)));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInParentGroup()
            throws IOException
    {
        ldapUserName = PARENT_GROUP_USER.getAttributes().get("cn");
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains(format("User [%s] not a member of an authorized group", ldapUserName)));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForOrphanLdapUser()
            throws IOException
    {
        ldapUserName = ORPHAN_USER.getAttributes().get("cn");
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains(format("User [%s] not a member of an authorized group", ldapUserName)));
    }

    @Test(groups = {LDAP, LDAP_CLI, LDAP_MULTIPLE_BINDS, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForFailedBind()
            throws IOException
    {
        ldapUserName = USER_IN_EUROPE.getAttributes().get("cn");
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Invalid credentials"));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapPassword()
            throws IOException
    {
        ldapUserPassword = "wrong_password";
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Invalid credentials"));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapUser()
            throws IOException
    {
        ldapUserName = "invalid_user";
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Access Denied"));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForEmptyUser()
            throws IOException
    {
        ldapUserName = "";
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Malformed credentials: user is empty"));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutPassword()
            throws IOException
    {
        launchTrinoCli("--server", ldapServerAddress,
                "--truststore-path", ldapTruststorePath,
                "--truststore-password", ldapTruststorePassword,
                "--user", ldapUserName,
                "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Authentication failed: Unauthorized"));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutHttps()
            throws IOException
    {
        ldapServerAddress = format("http://%s:8443", serverHost);
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Authentication using username/password requires HTTPS to be enabled"));
        skipAfterTestWithContext();
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForIncorrectTrustStore()
            throws IOException
    {
        ldapTruststorePassword = "wrong_password";
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Error setting up SSL: keystore password was incorrect"));
        skipAfterTestWithContext();
    }

    private void skipAfterTestWithContext()
    {
        trino.close();
        trino = null;
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassForCredentialsWithSpecialCharacters()
            throws IOException
    {
        ldapUserName = SPECIAL_USER.getAttributes().get("cn");
        ldapUserPassword = SPECIAL_USER.getAttributes().get("userPassword");
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForUserWithColon()
            throws IOException
    {
        ldapUserName = "UserWith:Colon";
        launchTrinoCliWithServerArgument("--execute", SELECT_FROM_NATION);
        assertThat(trimLines(trino.readRemainingErrorLines())).anySatisfy(line ->
                assertThat(line).contains("Illegal character ':' found in username"));
        skipAfterTestWithContext();
    }

    @Test(groups = {LDAP_AND_FILE_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryWithFileAuthenticator()
            throws IOException
    {
        ldapUserPassword = fileUserPassword;
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();
        trino.getProcessInput().println(SELECT_FROM_NATION);
        assertThat(trimLines(trino.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    @Test(groups = {LDAP_AND_FILE_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryForAnotherUserWithOnlyFileAuthenticator()
            throws IOException
    {
        ldapUserName = "OnlyFileUser";
        ldapUserPassword = onlyFileUserPassword;
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();
        trino.getProcessInput().println(SELECT_FROM_NATION);
        assertThat(trimLines(trino.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    private void launchTrinoCliWithServerArgument(String... arguments)
            throws IOException
    {
        requireNonNull(ldapTruststorePath, "ldapTruststorePath is null");
        requireNonNull(ldapTruststorePassword, "ldapTruststorePassword is null");
        requireNonNull(ldapUserName, "ldapUserName is null");
        requireNonNull(ldapServerAddress, "ldapServerAddress is null");
        requireNonNull(ldapUserPassword, "ldapUserPassword is null");

        ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
        prestoClientOptions.add(
                "--server", ldapServerAddress,
                "--truststore-path", ldapTruststorePath,
                "--truststore-password", ldapTruststorePassword,
                "--user", ldapUserName,
                "--password");

        prestoClientOptions.add(arguments);
        ProcessBuilder processBuilder = getProcessBuilder(prestoClientOptions.build());
        processBuilder.environment().put("TRINO_PASSWORD", ldapUserPassword);
        trino = new TrinoCliProcess(processBuilder.start());
    }
}
