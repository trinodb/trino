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
package io.prestosql.tests.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.tempto.Requirements.compose;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestosql.tempto.process.CliProcess.trimLines;
import static io.prestosql.tests.TestGroups.FILEAUTH;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class PrestoCliFileAuthTests
        extends PrestoCliLauncher
        implements RequirementsProvider
{
    @Inject(optional = true)
    @Named("databases.presto.cli_truststore_path")
    private String truststorePath;

    @Inject(optional = true)
    @Named("databases.presto.cli_truststore_password")
    private String truststorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_server_address")
    private String serverAddress;

    protected PrestoCliFileAuthTests()
            throws IOException
    {
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(immutableTable(NATION));
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testCorrectPBKDF2Password()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userPBKDF2", "password", "--execute", "select * from hive.default.nation;");
        assertSuccess();
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testIncorrectPBKDF2Password()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userPBKDF2", "passwordPBKDF2", "--execute", "select * from hive.default.nation;");
        assertAuthenticationError();
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testCorrectBCryptPassword()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userBCrypt", "user123", "--execute", "select * from hive.default.nation;");
        assertSuccess();
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testIncorrectBCryptPassword()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userBCrypt", "user1234", "--execute", "select * from hive.default.nation;");
        assertAuthenticationError();
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testAuthTokenCacheUpdate()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userBCrypt", "user123", "--execute", "select * from hive.default.nation;");
        assertSuccess();
        //update the content in file and check for success
        addContentToFile("/docker/volumes/conf/presto/etc/file_auth_copy", "userBCrypt1:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa");
        Thread.sleep(200); // pausing till refresh-period for reloading the file
        launchPrestoCliWithServerArgument("userBCrypt1", "user123", "--execute", "select * from hive.default.nation;");
        assertSuccess();
        //delete the content in file and check for failure
        deleteContentToFile("/docker/volumes/conf/presto/etc/file_auth_copy", "userBCrypt1:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa");
        launchPrestoCliWithServerArgument("userBCrypt1", "user123", "--execute", "select * from hive.default.nation;");
        assertAuthenticationError();
    }

    @Test(groups = {FILEAUTH, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void testNonExistentUserFailure()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("userDNE", "passwordXYZ", "--execute", "select * from hive.default.nation;");
        assertAuthenticationError();
    }

    private void assertSuccess()
    {
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        skipAfterTestWithContext();
    }

    private void assertAuthenticationError()
    {
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication failed")));
        skipAfterTestWithContext();
    }

    private void skipAfterTestWithContext()
    {
        if (presto != null) {
            presto.close();
            presto = null;
        }
    }

    private void addContentToFile(String filename, String content) throws IOException
    {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            writer.newLine();
            writer.write(content);
        }
        catch (Exception e) {
            throw e;
        }
    }

    private void deleteContentToFile(String filename, String content) throws IOException
    {
        List<String> stringStream;
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            stringStream = reader.lines().filter(s -> !s.contains(content)).collect(Collectors.toList());
        }
        catch (IOException e) {
            throw e;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, false))) {
            stringStream.stream().forEach(string -> {
                try {
                    writer.write(string);
                    writer.newLine();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (Exception e) {
            throw e;
        }
    }

    private void launchPrestoCliWithServerArgument(String username, String password, String... arguments)
            throws IOException, InterruptedException
    {
        requireNonNull(truststorePath, "databases.presto.cli_truststore_path is null");
        requireNonNull(truststorePassword, "databases.presto.cli_truststore_password is null");
        requireNonNull(serverAddress, "databases.presto.cli_server_address is null");

        List<String> prestoClientOptions = ImmutableList.builder()
                .add("--server", serverAddress)
                .add("--truststore-path", truststorePath,
                "--truststore-password", truststorePassword,
                "--user", username,
                "--password")
                .add(arguments)
                .build()
                .stream()
                .map(obj -> obj.toString())
                .collect(Collectors.toList());

        ProcessBuilder processBuilder = getProcessBuilder(prestoClientOptions);
        processBuilder.environment().put("PRESTO_PASSWORD", password);
        presto = new PrestoCliProcess(processBuilder.start());
    }
}
