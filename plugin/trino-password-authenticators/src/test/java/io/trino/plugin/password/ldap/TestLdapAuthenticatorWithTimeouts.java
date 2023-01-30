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
package io.trino.plugin.password.ldap;

import com.google.common.io.Closer;
import io.airlift.units.Duration;
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;
import io.trino.plugin.password.ldap.TestingOpenLdapServer.DisposableSubContext;
import io.trino.spi.security.BasicPrincipal;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static io.trino.plugin.password.ldap.TestingOpenLdapServer.LDAP_PORT;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestLdapAuthenticatorWithTimeouts
{
    private Closer closer;

    private TestingOpenLdapServer openLdapServer;
    private String proxyLdapUrl;

    @BeforeClass
    public void setup()
            throws Exception
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        ToxiproxyContainer proxyServer = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.4.0")
                .withNetwork(network);
        closer.register(proxyServer::close);
        proxyServer.start();

        openLdapServer = closer.register(new TestingOpenLdapServer(network));
        openLdapServer.start();

        ContainerProxy proxy = proxyServer.getProxy(openLdapServer.getNetworkAlias(), LDAP_PORT);
        proxy.toxics()
                .latency("latency", DOWNSTREAM, 5_000);
        proxyLdapUrl = format("ldap://%s:%s", proxy.getContainerIpAddress(), proxy.getProxyPort());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        closer.close();
        closer = null;
        openLdapServer = null;
        proxyLdapUrl = null;
    }

    @Test
    public void testConnectTimeout()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            LdapClientConfig ldapConfig = new LdapClientConfig()
                    .setLdapUrl(proxyLdapUrl)
                    .setLdapConnectionTimeout(new Duration(1, SECONDS));
            LdapAuthenticatorConfig ldapAuthenticatorConfig = new LdapAuthenticatorConfig()
                    .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName());

            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    new LdapAuthenticatorClient(
                            new JdkLdapClient(ldapConfig)),
                    ldapAuthenticatorConfig);
            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching(".*Authentication error.*");

            LdapClientConfig withIncreasedTimeout = ldapConfig.setLdapConnectionTimeout(new Duration(30, SECONDS));
            assertEquals(
                    new LdapAuthenticator(new LdapAuthenticatorClient(new JdkLdapClient(withIncreasedTimeout)), ldapAuthenticatorConfig)
                            .createAuthenticatedPrincipal("alice", "alice-pass"),
                    new BasicPrincipal("alice"));
        }
    }

    @Test
    public void testReadTimeout()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group = openLdapServer.createGroup(organization);
                DisposableSubContext alice = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            openLdapServer.addUserToGroup(alice, group);

            LdapClientConfig ldapConfig = new LdapClientConfig()
                    .setLdapUrl(proxyLdapUrl)
                    .setLdapReadTimeout(new Duration(1, SECONDS));

            LdapAuthenticatorConfig ldapAuthenticatorConfig = new LdapAuthenticatorConfig()
                    .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName())
                    .setUserBaseDistinguishedName(organization.getDistinguishedName())
                    .setGroupAuthorizationSearchPattern(format("(&(objectClass=groupOfNames)(cn=group_*)(member=uid=${USER},%s))", organization.getDistinguishedName()));

            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    new LdapAuthenticatorClient(
                            new JdkLdapClient(ldapConfig)),
                    ldapAuthenticatorConfig);
            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching(".*Authentication error.*");

            LdapClientConfig withIncreasedTimeout = ldapConfig.setLdapReadTimeout(new Duration(30, SECONDS));
            assertEquals(
                    new LdapAuthenticator(
                            new LdapAuthenticatorClient(
                                    new JdkLdapClient(withIncreasedTimeout)),
                            ldapAuthenticatorConfig)
                            .createAuthenticatedPrincipal("alice", "alice-pass"),
                    new BasicPrincipal("alice"));
        }
    }
}
