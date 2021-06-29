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

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.testing.TempFile;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.process.CliProcess.trimLines;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.CLI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestTrinoCli
        extends TrinoCliLauncher
        implements RequirementsProvider
{
    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_authentication")
    private boolean kerberosAuthentication;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_principal")
    private String kerberosPrincipal;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_keytab")
    private String kerberosKeytab;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_config_path")
    private String kerberosConfigPath;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_service_name")
    private String kerberosServiceName;

    @Inject(optional = true)
    @Named("databases.presto.https_keystore_path")
    private String keystorePath;

    @Inject(optional = true)
    @Named("databases.presto.https_keystore_password")
    private String keystorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_use_canonical_hostname")
    private boolean kerberosUseCanonicalHostname;

    @Inject
    @Named("databases.presto.jdbc_user")
    private String jdbcUser;

    public TestTrinoCli()
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
        return new ImmutableTableRequirement(NATION);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldDisplayVersion()
            throws IOException
    {
        launchTrinoCli("--version");
        assertThat(trino.readRemainingOutputLines()).containsExactly("Trino CLI " + readPrestoCliVersion());
    }

    private static String readPrestoCliVersion()
    {
        try {
            String version = Resources.toString(Resources.getResource("trino-cli-version.txt"), UTF_8).trim();
            checkState(!version.isEmpty(), "version is empty");
            return version;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQuery()
            throws IOException
    {
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();
        trino.getProcessInput().println("select * from hive.default.nation;");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunBatchQuery()
            throws Exception
    {
        launchTrinoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunBatchQueryWithStdinRedirect()
            throws Exception
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nation;", file.file(), UTF_8);

            launchTrinoCliWithRedirectedStdin(file.file());
            assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
            trino.waitForWithTimeoutAndKill();
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunMultipleBatchQueriesWithStdinRedirect()
            throws Exception
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nation;select 1;select 2", file.file(), UTF_8);

            launchTrinoCliWithRedirectedStdin(file.file());

            List<String> expectedLines = ImmutableList.<String>builder()
                    .addAll(nationTableBatchLines)
                    .add("\"1\"")
                    .add("\"2\"")
                    .build();

            assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(expectedLines);
            trino.waitForWithTimeoutAndKill();
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExecuteEmptyListOfStatements()
            throws Exception
    {
        launchTrinoCliWithServerArgument("--execute", "");
        assertTrue(trimLines(trino.readRemainingOutputLines()).isEmpty());
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldPassSessionUser()
            throws Exception
    {
        launchTrinoCliWithServerArgument("--session-user", "other-user", "--execute", "SELECT current_user;");
        assertThat(trimLines(trino.readRemainingOutputLines())).contains("\"other-user\"");
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldUseCatalogAndSchemaOptions()
            throws Exception
    {
        launchTrinoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldUseCatalogAndSchemaOptionsFromConfigFile()
            throws Exception
    {
        launchTrinoCliWithConfigurationFile(ImmutableList.of("catalog", "hive", "schema", "default"), "--execute", "SELECT * FROM nation;");
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldPreferCommandLineArgumentOverConfigDefault()
            throws Exception
    {
        launchTrinoCliWithConfigurationFile(ImmutableList.of("catalog", "some-other-catalog", "schema", "default"), "--catalog", "hive", "--execute", "SELECT * FROM nation;");
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        trino.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExitWithErrorOnUnknownPropertiesInConfigFile()
            throws Exception
    {
        String configPath = launchTrinoCliWithConfigurationFile(ImmutableList.of("unknown", "property", "catalog", "hive"));
        assertThat(trimLines(trino.readRemainingErrorLines())).containsExactly(format("Configuration file %s contains unknown properties [unknown]", configPath));

        assertThatThrownBy(() -> trino.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQueryFromFile()
            throws Exception
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nation;\n", file.file(), UTF_8);

            launchTrinoCliWithServerArgument("--file", file.file().getAbsolutePath());
            assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);

            trino.waitForWithTimeoutAndKill();
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExitOnErrorFromExecute()
            throws IOException
    {
        String sql = "select * from hive.default.nations; select * from hive.default.nation;";
        launchTrinoCliWithServerArgument("--execute", sql);
        assertThat(trimLines(trino.readRemainingOutputLines())).isEmpty();

        assertThatThrownBy(() -> trino.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExitOnErrorFromFile()
            throws IOException
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nations;\nselect * from hive.default.nation;\n", file.file(), UTF_8);

            launchTrinoCliWithServerArgument("--file", file.file().getAbsolutePath());
            assertThat(trimLines(trino.readRemainingOutputLines())).isEmpty();

            assertThatThrownBy(() -> trino.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldNotExitOnErrorFromExecute()
            throws IOException
    {
        String sql = "select * from hive.default.nations; select * from hive.default.nation;";
        launchTrinoCliWithServerArgument("--execute", sql, "--ignore-errors", "true");
        assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);

        assertThatThrownBy(() -> trino.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldNotExitOnErrorFromFile()
            throws IOException
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nations;\nselect * from hive.default.nation;\n", file.file(), UTF_8);

            launchTrinoCliWithServerArgument("--file", file.file().getAbsolutePath(), "--ignore-errors", "true");
            assertThat(trimLines(trino.readRemainingOutputLines())).containsAll(nationTableBatchLines);

            assertThatThrownBy(() -> trino.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldHandleSession()
            throws IOException
    {
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();

        trino.getProcessInput().println("use hive.default;");
        assertThat(trino.readLinesUntilPrompt()).contains("USE");

        trino.getProcessInput().println("select * from nation;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);

        trino.getProcessInput().println("show session;");
        assertThat(squeezeLines(trino.readLinesUntilPrompt()))
                .contains("join_distribution_type|AUTOMATIC|AUTOMATIC|varchar|Join distribution type. Possible values: [BROADCAST, PARTITIONED, AUTOMATIC]");

        trino.getProcessInput().println("set session join_distribution_type = 'BROADCAST';");
        assertThat(trino.readLinesUntilPrompt()).contains("SET SESSION");

        trino.getProcessInput().println("show session;");
        assertThat(squeezeLines(trino.readLinesUntilPrompt()))
                .contains("join_distribution_type|BROADCAST|AUTOMATIC|varchar|Join distribution type. Possible values: [BROADCAST, PARTITIONED, AUTOMATIC]");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldHandleTransaction()
            throws IOException
    {
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();

        trino.getProcessInput().println("use hive.default;");
        assertThat(trino.readLinesUntilPrompt()).contains("USE");

        // start transaction and create table
        trino.getProcessInput().println("start transaction;");
        assertThat(trino.readLinesUntilPrompt()).contains("START TRANSACTION");

        trino.getProcessInput().println("create table txn_test (x bigint);");
        assertThat(trino.readLinesUntilPrompt()).contains("CREATE TABLE");

        // cause an error that aborts the transaction
        trino.getProcessInput().println("select foo;");
        assertThat(trino.readLinesUntilPrompt()).extracting(TestTrinoCli::removePrefix)
                .contains("line 1:8: Column 'foo' cannot be resolved");

        // verify commands are rejected until rollback
        trino.getProcessInput().println("select * from nation;");
        assertThat(trino.readLinesUntilPrompt()).extracting(TestTrinoCli::removePrefix)
                .contains("Current transaction is aborted, commands ignored until end of transaction block");

        trino.getProcessInput().println("rollback;");
        assertThat(trino.readLinesUntilPrompt()).contains("ROLLBACK");

        // verify commands work after rollback
        trino.getProcessInput().println("select * from nation;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);

        // verify table was not created
        trino.getProcessInput().println("show tables;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).doesNotContain("txn_test");

        // start transaction, create two tables and commit
        trino.getProcessInput().println("start transaction;");
        assertThat(trino.readLinesUntilPrompt()).contains("START TRANSACTION");

        trino.getProcessInput().println("create table txn_test1 (x bigint);");
        assertThat(trino.readLinesUntilPrompt()).contains("CREATE TABLE");

        trino.getProcessInput().println("create table txn_test2 (x bigint);");
        assertThat(trino.readLinesUntilPrompt()).contains("CREATE TABLE");

        trino.getProcessInput().println("commit;");
        assertThat(trino.readLinesUntilPrompt()).contains("COMMIT");

        // verify tables were created
        trino.getProcessInput().println("show tables;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("txn_test1", "txn_test2");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testSetRole()
            throws IOException
    {
        launchTrinoCliWithServerArgument();
        trino.waitForPrompt();

        trino.getProcessInput().println("use hive.default;");
        assertThat(trino.readLinesUntilPrompt()).contains("USE");

        trino.getProcessInput().println("show current roles;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("public");

        trino.getProcessInput().println("set role admin;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("SET ROLE");
        trino.getProcessInput().println("show current roles;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("public", "admin");

        trino.getProcessInput().println("set role none;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("SET ROLE");
        trino.getProcessInput().println("show current roles;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).doesNotContain("admin");
    }

    private void launchTrinoCliWithServerArgument(String... arguments)
            throws IOException
    {
        launchTrinoCli(getTrinoCliArguments(arguments));
    }

    private String launchTrinoCliWithConfigurationFile(List<String> configuration, String... arguments)
            throws IOException
    {
        CharMatcher matcher = CharMatcher.is('-');
        ProcessBuilder processBuilder = getProcessBuilder(ImmutableList.copyOf(arguments));

        String fileContent = getTrinoCliArguments(configuration.toArray(new String[0]))
                .stream()
                .map(matcher::trimLeadingFrom)
                .collect(joining("\n"));

        File tempFile = File.createTempFile(".trino_config", "");
        tempFile.deleteOnExit();
        Files.write(fileContent.getBytes(UTF_8), tempFile);

        processBuilder.environment().put("TRINO_CONFIG", tempFile.getAbsolutePath());
        trino = new TrinoCliProcess(processBuilder.start());

        return tempFile.getPath();
    }

    private void launchTrinoCliWithRedirectedStdin(File inputFile)
            throws IOException
    {
        ProcessBuilder processBuilder = getProcessBuilder(getTrinoCliArguments());
        processBuilder.redirectInput(inputFile);
        trino = new TrinoCliProcess(processBuilder.start());
    }

    private List<String> getTrinoCliArguments(String... arguments)
    {
        verify(arguments.length % 2 == 0, "arguments.length should be divisible by 2");

        ImmutableList.Builder<String> trinoClientOptions = ImmutableList.builder();
        trinoClientOptions.add("--server", serverAddress);
        trinoClientOptions.add("--user", jdbcUser);

        if (keystorePath != null) {
            trinoClientOptions.add("--keystore-path", keystorePath);
        }

        if (keystorePassword != null) {
            trinoClientOptions.add("--keystore-password", keystorePassword);
        }

        if (kerberosAuthentication) {
            requireNonNull(kerberosPrincipal, "kerberosPrincipal is null");
            requireNonNull(kerberosKeytab, "kerberosKeytab is null");
            requireNonNull(kerberosServiceName, "kerberosServiceName is null");
            requireNonNull(kerberosConfigPath, "kerberosConfigPath is null");

            trinoClientOptions.add("--krb5-principal", kerberosPrincipal);
            trinoClientOptions.add("--krb5-keytab-path", kerberosKeytab);
            trinoClientOptions.add("--krb5-remote-service-name", kerberosServiceName);
            trinoClientOptions.add("--krb5-config-path", kerberosConfigPath);

            if (!kerberosUseCanonicalHostname) {
                trinoClientOptions.add("--krb5-disable-remote-service-hostname-canonicalization", "true");
            }
        }

        trinoClientOptions.add(arguments);

        return Lists.partition(trinoClientOptions.build(), 2)
                .stream()
                .map(argument -> format("%s=%s", argument.get(0), argument.get(1)))
                .collect(toImmutableList());
    }

    private static String removePrefix(String line)
    {
        int i = line.indexOf(':');
        return (i >= 0) ? line.substring(i + 1).trim() : line;
    }

    public static List<String> squeezeLines(List<String> lines)
    {
        return lines.stream()
                .map(line -> line.replaceAll(" +\\| +", "|").trim())
                .collect(toList());
    }
}
