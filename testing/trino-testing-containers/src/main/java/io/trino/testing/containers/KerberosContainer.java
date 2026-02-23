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
package io.trino.testing.containers;

import io.trino.testing.TestingProperties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A Kerberos Key Distribution Center (KDC) container for testing.
 * <p>
 * This container provides a standalone KDC that can be used to enable Kerberos
 * authentication for other containers in the same Docker network. It uses the
 * {@code ghcr.io/trinodb/testing/kdc} image which includes tools for
 * creating principals and keytabs at runtime.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * KerberosContainer kdc = new KerberosContainer()
 *     .withNetwork(network)
 *     .withPrincipal("hdfs/hadoop-master", "/keytabs/hdfs.keytab")
 *     .withPrincipal("hive/hadoop-master", "/keytabs/hive.keytab");
 * kdc.start();
 *
 * // Get krb5.conf for clients
 * String krb5Conf = kdc.getKrb5Conf();
 *
 * // Get keytab bytes
 * byte[] hdfsKeytab = kdc.getKeytab("/keytabs/hdfs.keytab");
 * }</pre>
 */
public class KerberosContainer
        extends GenericContainer<KerberosContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/kdc";

    public static final String HOST_NAME = "kdc";
    public static final int KDC_PORT = 88;
    // This must match the realm configured in the KDC image
    public static final String REALM = "TRINO.TEST";

    private final List<PrincipalSpec> principals = new ArrayList<>();

    public KerberosContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public KerberosContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(KDC_PORT);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST_NAME));
        // Wait for KDC to be listening
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Adds a principal to be created when the container starts.
     * <p>
     * The principal will be created using the {@code create_principal} script
     * built into the KDC image. The keytab will be written to the specified path
     * inside the container and can be retrieved using {@link #getKeytab(String)}.
     *
     * @param principal the principal name (e.g., "hdfs/hadoop-master")
     * @param keytabPath the path inside the container where the keytab should be written
     * @return this container for method chaining
     */
    public KerberosContainer withPrincipal(String principal, String keytabPath)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(keytabPath, "keytabPath is null");
        principals.add(new PrincipalSpec(principal, keytabPath));
        return this;
    }

    @Override
    public void start()
    {
        super.start();

        // Create principals after container is started
        for (PrincipalSpec spec : principals) {
            createPrincipal(spec.principal(), spec.keytabPath());
        }
    }

    /**
     * Creates a principal and keytab in the running container.
     *
     * @param principal the principal name
     * @param keytabPath the path where the keytab should be written
     */
    public void createPrincipal(String principal, String keytabPath)
    {
        try {
            // Ensure the parent directory exists
            String parentDir = keytabPath.substring(0, keytabPath.lastIndexOf('/'));
            ExecResult mkdirResult = execInContainer("mkdir", "-p", parentDir);
            if (mkdirResult.getExitCode() != 0) {
                throw new RuntimeException("Failed to create directory " + parentDir + ": " + mkdirResult.getStderr());
            }

            // Create the principal and keytab using the create_principal script
            // Note: The script adds the realm automatically, so don't include it in the principal
            ExecResult result = execInContainer(
                    "/usr/local/bin/create_principal",
                    "-p", principal,
                    "-k", keytabPath);
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to create principal " + principal + ": " + result.getStderr());
            }
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to create principal " + principal, e);
        }
    }

    /**
     * Returns the keytab file contents from the container.
     *
     * @param keytabPath the path to the keytab inside the container
     * @return the keytab file contents as bytes
     */
    public byte[] getKeytab(String keytabPath)
    {
        try {
            return copyFileFromContainer(keytabPath, InputStream::readAllBytes);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get keytab from " + keytabPath, e);
        }
    }

    /**
     * Creates a credential cache file from a keytab using kinit.
     * <p>
     * The credential cache can be used instead of a keytab for Kerberos authentication.
     * This is useful for testing Trino's credential cache support.
     *
     * @param principal the fully qualified principal name (e.g., "trino/trino-master@TRINO.TEST")
     * @param keytabPath the path to the keytab inside the container
     * @return the credential cache file contents as bytes
     */
    public byte[] createCredentialCache(String principal, String keytabPath)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(keytabPath, "keytabPath is null");

        String credentialCachePath = "/tmp/krb5cc_" + System.currentTimeMillis();
        try {
            // Write krb5.conf that points to localhost (for use inside the KDC container)
            String localKrb5Conf = """
                    [libdefaults]
                      default_realm = %s
                      dns_lookup_realm = false
                      dns_lookup_kdc = false
                    [realms]
                      %s = {
                        kdc = localhost:%d
                      }
                    """.formatted(REALM, REALM, KDC_PORT);

            String krb5ConfPath = "/tmp/krb5_local.conf";
            ExecResult writeResult = execInContainer("sh", "-c", "cat > " + krb5ConfPath + " << 'EOF'\n" + localKrb5Conf + "EOF");
            if (writeResult.getExitCode() != 0) {
                throw new RuntimeException("Failed to write krb5.conf: " + writeResult.getStderr());
            }

            // Run kinit with the local krb5.conf to create credential cache from keytab
            ExecResult result = execInContainer(
                    "sh", "-c",
                    "KRB5_CONFIG=" + krb5ConfPath + " kinit -k -t " + keytabPath + " -c " + credentialCachePath + " " + principal);
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to create credential cache for " + principal +
                        ": " + result.getStderr() + " " + result.getStdout());
            }

            // Read and return the credential cache file
            return copyFileFromContainer(credentialCachePath, InputStream::readAllBytes);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to create credential cache for " + principal, e);
        }
    }

    /**
     * Returns krb5.conf content for clients to use with this KDC.
     * <p>
     * This configuration uses the internal Docker network hostname for KDC access.
     * Use this for containers in the same Docker network.
     *
     * @return krb5.conf content
     */
    public String getKrb5Conf()
    {
        return """
                [logging]
                  default = FILE:/var/log/krb5libs.log
                  kdc = FILE:/var/log/krb5kdc.log
                  admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                  default_realm = %s
                  dns_lookup_realm = false
                  dns_lookup_kdc = false
                  forwardable = true
                  rdns = false

                [realms]
                  %s = {
                    kdc = %s:%d
                    admin_server = %s
                  }
                """.formatted(REALM, REALM, HOST_NAME, KDC_PORT, HOST_NAME);
    }

    /**
     * Returns krb5.conf content for external clients (outside Docker network).
     * <p>
     * This configuration uses the mapped host port for KDC access.
     * Use this for clients running on the host machine (e.g., test code).
     *
     * @return krb5.conf content for external use
     */
    public String getExternalKrb5Conf()
    {
        return """
                [logging]
                  default = FILE:/var/log/krb5libs.log
                  kdc = FILE:/var/log/krb5kdc.log
                  admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                  default_realm = %s
                  dns_lookup_realm = false
                  dns_lookup_kdc = false
                  forwardable = true
                  rdns = false

                [realms]
                  %s = {
                    kdc = %s:%d
                    admin_server = %s
                  }
                """.formatted(REALM, REALM, getHost(), getMappedPort(KDC_PORT), getHost());
    }

    /**
     * Returns the Kerberos realm name.
     */
    public String getRealm()
    {
        return REALM;
    }

    /**
     * Returns a fully qualified principal name.
     *
     * @param principal the principal name without realm (e.g., "hdfs/hadoop-master")
     * @return the fully qualified principal (e.g., "hdfs/hadoop-master@TESTING.TRINO.IO")
     */
    public String getFullPrincipal(String principal)
    {
        return principal + "@" + REALM;
    }

    private record PrincipalSpec(String principal, String keytabPath) {}
}
