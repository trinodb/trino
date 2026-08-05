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
package io.trino.filesystem.hdfs;

import com.google.common.net.HostAndPort;
import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.PrintingLogConsumer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingProperties.getDockerImagesVersion;

public class KerberosKdc
        extends BaseTestContainer
{
    private static final String IMAGE = "ghcr.io/trinodb/testing/kdc:" + getDockerImagesVersion();

    private static final int KDC_PORT = 88;

    public KerberosKdc(Network network)
    {
        super(IMAGE,
                "kdc",
                Set.of(KDC_PORT),
                Map.of(),
                Map.of(),
                Optional.of(network),
                1);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withLogConsumer(new PrintingLogConsumer("KerberosKdc"));
    }

    public HostAndPort getKdcAddress()
    {
        return getMappedHostAndPortForExposedPort(KDC_PORT);
    }

    public void createKeytabs(Path keytabDirectory)
            throws Exception
    {
        Files.createDirectories(keytabDirectory);
        executeInContainerFailOnError("mkdir", "-p", "/tmp/keytabs");

        createKeytab("hdfs", "hdfs/hadoop-master", keytabDirectory);
        createKeytab("hive", "hive/hadoop-master", keytabDirectory);
        createKeytab("HTTP", "HTTP/hadoop-master", keytabDirectory);
        createKeytab("mapred", "mapred/hadoop-master", keytabDirectory);
        createKeytab("yarn", "yarn/hadoop-master", keytabDirectory);
    }

    public void printDiagnostics()
    {
        printDiagnostic("supervisor status", "supervisorctl", "status");
        printDiagnostic("KDC log", "bash", "-c", "tail -n 500 /var/log/krb5kdc.log 2>&1 || true");
        printDiagnostic("kadmind log", "bash", "-c", "tail -n 500 /var/log/kadmind.log 2>&1 || true");
    }

    private void createKeytab(String keytabName, String principal, Path keytabDirectory)
            throws Exception
    {
        String containerKeytab = "/tmp/keytabs/%s.keytab".formatted(keytabName);
        executeInContainerFailOnError("create_principal", "-p", principal, "-k", containerKeytab);
        String encodedKeytab = executeInContainerFailOnError("base64", "-w0", containerKeytab);
        Files.write(keytabDirectory.resolve("%s.keytab".formatted(keytabName)), Base64.getDecoder().decode(encodedKeytab));
    }

    private void printDiagnostic(String name, String... command)
    {
        try {
            Container.ExecResult result = executeInContainer(command);
            System.err.printf(
                    "KerberosKdc diagnostic: %s%ncommand: %s%nexit code: %s%nstdout:%n%s%nstderr:%n%s%n",
                    name,
                    String.join(" ", command),
                    result.getExitCode(),
                    result.getStdout(),
                    result.getStderr());
        }
        catch (RuntimeException e) {
            System.err.printf("Failed to print KerberosKdc diagnostic: %s%n%s%n", name, e);
        }
    }
}
