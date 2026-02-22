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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterMethodWithContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.tempto.process.CliProcess.trimLines;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoCliRole
        extends TrinoCliLauncher
{
    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_authentication")
    private boolean kerberosAuthentication;

    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_principal")
    private String kerberosPrincipal;

    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_keytab")
    private String kerberosKeytab;

    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_config_path")
    private String kerberosConfigPath;

    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_service_name")
    private String kerberosServiceName;

    @Inject(optional = true)
    @Named("databases.trino.https_keystore_path")
    private String keystorePath;

    @Inject(optional = true)
    @Named("databases.trino.https_keystore_password")
    private String keystorePassword;

    @Inject(optional = true)
    @Named("databases.trino.cli_kerberos_use_canonical_hostname")
    private boolean kerberosUseCanonicalHostname;

    @Inject
    @Named("databases.trino.jdbc_user")
    private String jdbcUser;

    public TestTrinoCliRole()
            throws IOException
    {}

    @AfterMethodWithContext
    @Override
    public void stopCli()
            throws InterruptedException
    {
        super.stopCli();
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testSetRole()
            throws IOException
    {
        launchTrinoCli(getTrinoCliArguments());
        trino.waitForPrompt();

        trino.getProcessInput().println("use hive.default;");
        assertThat(trino.readLinesUntilPrompt()).contains("USE");

        trino.getProcessInput().println("show current roles from hive;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("public");

        trino.getProcessInput().println("set role admin in hive;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("SET ROLE");
        trino.getProcessInput().println("show current roles from hive;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("public", "admin");

        trino.getProcessInput().println("set role none in hive;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).contains("SET ROLE");
        trino.getProcessInput().println("show current roles from hive;");
        assertThat(trimLines(trino.readLinesUntilPrompt())).doesNotContain("admin");
    }

    private List<String> getTrinoCliArguments(String... arguments)
    {
        verify(arguments.length % 2 == 0, "arguments.length should be divisible by 2");

        ImmutableList.Builder<String> options = ImmutableList.builder();
        options.add("--server", serverAddress);
        options.add("--user", jdbcUser);

        if (keystorePath != null) {
            options.add("--keystore-path", keystorePath);
        }

        if (keystorePassword != null) {
            options.add("--keystore-password", keystorePassword);
        }

        if (kerberosAuthentication) {
            requireNonNull(kerberosPrincipal, "kerberosPrincipal is null");
            requireNonNull(kerberosKeytab, "kerberosKeytab is null");
            requireNonNull(kerberosServiceName, "kerberosServiceName is null");
            requireNonNull(kerberosConfigPath, "kerberosConfigPath is null");

            options.add("--krb5-principal", kerberosPrincipal);
            options.add("--krb5-keytab-path", kerberosKeytab);
            options.add("--krb5-remote-service-name", kerberosServiceName);
            options.add("--krb5-config-path", kerberosConfigPath);

            if (!kerberosUseCanonicalHostname) {
                options.add("--krb5-disable-remote-service-hostname-canonicalization", "true");
            }
        }

        options.add(arguments);

        return Lists.partition(options.build(), 2).stream()
                .map(argument -> format("%s=%s", argument.get(0), argument.get(1)))
                .toList();
    }
}
