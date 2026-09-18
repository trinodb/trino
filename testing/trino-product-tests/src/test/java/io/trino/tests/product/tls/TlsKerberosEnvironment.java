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
package io.trino.tests.product.tls;

import io.trino.testing.containers.KerberosContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class TlsKerberosEnvironment
        extends TlsEnvironment
{
    private static final String CLIENT_PRINCIPAL = "trino-client/" + COORDINATOR_FQDN;
    private static final String SERVER_PRINCIPAL = "trino-server/" + COORDINATOR_FQDN;
    private static final String KDC_CLIENT_KEYTAB_PATH = "/keytabs/trino-client.keytab";
    private static final String KDC_SERVER_KEYTAB_PATH = "/keytabs/trino-server.keytab";
    private static final String TRINO_SERVER_KEYTAB_PATH = "/etc/trino/trino-server.keytab";

    private KerberosContainer kdc;
    private Path clientKeytab;
    private byte[] serverKeytab;
    private Path externalKrb5Config;

    @Override
    protected void startDependencies(Network network)
    {
        kdc = new KerberosContainer()
                .withNetwork(network)
                .withNetworkAliases(KerberosContainer.HOST_NAME)
                .withPrincipal(CLIENT_PRINCIPAL, KDC_CLIENT_KEYTAB_PATH)
                .withPrincipal(SERVER_PRINCIPAL, KDC_SERVER_KEYTAB_PATH);
        kdc.start();

        try {
            clientKeytab = Files.createTempFile("tls-kerberos-client-", ".keytab");
            Files.write(clientKeytab, kdc.getKeytab(KDC_CLIENT_KEYTAB_PATH));
            serverKeytab = kdc.getKeytab(KDC_SERVER_KEYTAB_PATH);
            externalKrb5Config = Files.createTempFile("tls-kerberos-", ".conf");
            Files.writeString(externalKrb5Config, createExternalKrb5Config());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare TLS Kerberos files", e);
        }
    }

    @Override
    protected String authenticationConfig()
    {
        return """
               http.authentication.krb5.config=/etc/krb5.conf
               http-server.authentication.type=KERBEROS
               http-server.authentication.krb5.service-name=trino-server
               http-server.authentication.krb5.principal-hostname=%s
               http-server.authentication.krb5.keytab=%s
               """.formatted(COORDINATOR_FQDN, TRINO_SERVER_KEYTAB_PATH);
    }

    @Override
    protected void configureContainer(GenericContainer<?> container)
    {
        container.withCopyToContainer(Transferable.of(kdc.getKrb5Conf()), "/etc/krb5.conf");
        container.withCopyToContainer(Transferable.of(serverKeytab, 0644), TRINO_SERVER_KEYTAB_PATH);
    }

    @Override
    protected void configureConnectionProperties(Properties properties)
    {
        properties.setProperty("user", CLIENT_PRINCIPAL + "@" + KerberosContainer.REALM);
        properties.setProperty("KerberosRemoteServiceName", "trino-server");
        properties.setProperty("KerberosServicePrincipalPattern", "${SERVICE}@" + COORDINATOR_FQDN);
        properties.setProperty("KerberosUseCanonicalHostname", "false");
        properties.setProperty("KerberosPrincipal", CLIENT_PRINCIPAL + "@" + KerberosContainer.REALM);
        properties.setProperty("KerberosConfigPath", externalKrb5Config.toString());
        properties.setProperty("KerberosKeytabPath", clientKeytab.toString());
    }

    @Override
    protected void closeDependencies()
    {
        deleteIfExists(clientKeytab);
        clientKeytab = null;
        serverKeytab = null;
        deleteIfExists(externalKrb5Config);
        externalKrb5Config = null;

        if (kdc != null) {
            kdc.close();
            kdc = null;
        }
    }

    private static void deleteIfExists(Path path)
    {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException ignored) {
            // Temporary test credentials are cleaned up on a best-effort basis.
        }
    }

    private String createExternalKrb5Config()
    {
        return kdc.getExternalKrb5Conf().replace(
                "  forwardable = true\n",
                "  forwardable = true\n  udp_preference_limit = 1\n");
    }
}
