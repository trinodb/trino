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
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A Hadoop KMS (Key Management Server) container for testing HDFS encryption zones.
 * <p>
 * This container runs the Hadoop KMS service, which provides transparent encryption
 * for HDFS. It reuses the same Docker image as {@link HadoopContainer} since it contains
 * a full Hadoop installation at {@code /opt/hadoop}.
 * <p>
 * The KMS runs on port 9600 and provides a REST API for key management operations.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * KmsContainer kms = new KmsContainer()
 *     .withNetwork(network)
 *     .withKmsSiteXml(kmsSiteXml)
 *     .withCoreSiteXml(coreSiteXml);
 * kms.start();
 * }</pre>
 * <p>
 * <b>Kerberos Support:</b>
 * <p>
 * For Kerberos-enabled environments, you must configure:
 * <ul>
 *   <li>KMS principal in kms-site.xml</li>
 *   <li>Kerberos authentication in core-site.xml</li>
 *   <li>Keytab file mounted to the container</li>
 * </ul>
 */
public class KmsContainer
        extends GenericContainer<KmsContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hive3.1";

    public static final String HOST_NAME = "kms";
    public static final int KMS_PORT = 9600;

    // Default non-kerberized kms-site.xml
    private static final String DEFAULT_KMS_SITE_XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>
                <!-- JavaKeyStoreProvider for key storage -->
                <property>
                    <name>hadoop.kms.key.provider.uri</name>
                    <value>jceks://file@/var/lib/kms/keys.jceks</value>
                </property>

                <!-- ACLs - allow all operations for testing -->
                <property>
                    <name>hadoop.kms.acl.CREATE</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.DELETE</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.ROLLOVER</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.GET</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.GET_KEYS</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.GET_METADATA</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.SET_KEY_MATERIAL</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.GENERATE_EEK</name>
                    <value>*</value>
                </property>
                <property>
                    <name>hadoop.kms.acl.DECRYPT_EEK</name>
                    <value>*</value>
                </property>
            </configuration>
            """;

    private String kmsSiteXml;
    private String coreSiteXml;
    private boolean kerberosEnabled;

    public KmsContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public KmsContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(KMS_PORT);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST_NAME));
        withEnv("TZ", "UTC");
        // Override the entrypoint to run KMS directly instead of supervisord
        withCommand("/bin/bash", "-c", """
                # Source Hadoop environment
                export HADOOP_HOME=/opt/hadoop
                export PATH=$HADOOP_HOME/bin:$PATH
                export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

                # Set KMS-specific environment
                export KMS_HOME=$HADOOP_HOME
                export KMS_CONFIG=$HADOOP_HOME/etc/hadoop

                # Create keystore directory
                mkdir -p /var/lib/kms
                chmod 755 /var/lib/kms

                # Wait for any bind-mounted config files to be ready
                sleep 2

                echo "Starting Hadoop KMS..."

                # Run KMS in foreground
                exec $HADOOP_HOME/bin/hadoop kms
                """);
        // Wait for KMS to be ready - look for Jersey initialization message
        // Note: We can't use HTTP wait because Kerberos auth may be required
        waitingFor(Wait.forLogMessage(".*Initiating Jersey application.*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    /**
     * Enables Kerberos authentication for the KMS server.
     * <p>
     * When Kerberos is enabled, you must also configure:
     * <ul>
     *   <li>A keytab file mounted to the container</li>
     *   <li>The appropriate kms-site.xml with principal configuration</li>
     * </ul>
     *
     * @return this container for method chaining
     */
    public KmsContainer withKerberosEnabled()
    {
        this.kerberosEnabled = true;
        return this;
    }

    /**
     * Sets the kms-site.xml configuration for the KMS server.
     * <p>
     * This configuration specifies:
     * <ul>
     *   <li>Keystore backend type and location</li>
     *   <li>Kerberos principal and keytab (for Kerberized environments)</li>
     *   <li>ACLs for key management operations</li>
     * </ul>
     *
     * @param kmsSiteXml the XML configuration content
     * @return this container for method chaining
     */
    public KmsContainer withKmsSiteXml(String kmsSiteXml)
    {
        this.kmsSiteXml = kmsSiteXml;
        return this;
    }

    /**
     * Sets the core-site.xml configuration for the KMS server.
     * <p>
     * This configuration specifies:
     * <ul>
     *   <li>Kerberos authentication settings</li>
     *   <li>Proxy user configurations for impersonation</li>
     * </ul>
     *
     * @param coreSiteXml the XML configuration content
     * @return this container for method chaining
     */
    public KmsContainer withCoreSiteXml(String coreSiteXml)
    {
        this.coreSiteXml = coreSiteXml;
        return this;
    }

    @Override
    public void start()
    {
        // Use default or provided kms-site.xml
        String effectiveKmsSiteXml = kmsSiteXml != null ? kmsSiteXml : DEFAULT_KMS_SITE_XML;
        withCopyToContainer(
                Transferable.of(effectiveKmsSiteXml),
                "/opt/hadoop/etc/hadoop/kms-site.xml");

        if (coreSiteXml != null) {
            withCopyToContainer(
                    Transferable.of(coreSiteXml),
                    "/opt/hadoop/etc/hadoop/core-site.xml");
        }
        super.start();
    }

    /**
     * Returns whether Kerberos authentication is enabled.
     */
    public boolean isKerberosEnabled()
    {
        return kerberosEnabled;
    }

    /**
     * Returns the KMS URL for clients within the Docker network.
     */
    public String getKmsUrl()
    {
        return "kms://http@" + HOST_NAME + ":" + KMS_PORT + "/kms";
    }

    /**
     * Returns the externally accessible KMS URL for connecting from the host.
     */
    public String getExternalKmsUrl()
    {
        return "kms://http@" + getHost() + ":" + getMappedPort(KMS_PORT) + "/kms";
    }

    /**
     * Returns the key provider path configuration value for Hadoop.
     * <p>
     * This value should be used in core-site.xml as:
     * <pre>{@code
     * <property>
     *   <name>hadoop.security.key.provider.path</name>
     *   <value>kms://http@kms:9600/kms</value>
     * </property>
     * }</pre>
     */
    public String getKeyProviderPath()
    {
        return "kms://http@" + HOST_NAME + ":" + KMS_PORT + "/kms";
    }
}
