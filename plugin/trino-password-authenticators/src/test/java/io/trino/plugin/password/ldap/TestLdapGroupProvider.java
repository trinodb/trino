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
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;
import io.trino.plugin.base.ldap.LdapUtil;
import io.trino.plugin.password.ldap.TestingOpenLdapServer.DisposableSubContext;
import org.testcontainers.containers.Network;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TestLdapGroupProvider
{
    private final Closer closer = Closer.create();
    private static final Pattern EXTRACT_GROUP_PATTERN = Pattern.compile("(?i)cn=([^,]*),(.*)$");

    private TestingOpenLdapServer openLdapServer;
    private LdapClient client;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Network network = Network.newNetwork();
        closer.register(network::close);

        openLdapServer = new TestingOpenLdapServer(network);
        closer.register(openLdapServer);
        openLdapServer.start();

        client = new JdkLdapClient(new LdapClientConfig()
                        .setLdapUrl(openLdapServer.getLdapUrl()));
    }

    @Test
    public void testGetGroups()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group = openLdapServer.createGroup(organization);
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            openLdapServer.addUserToGroup(ignored, group);
            String groupName = "";
            Matcher m = EXTRACT_GROUP_PATTERN.matcher(group.getDistinguishedName());
            if (m.find()) {
                groupName = m.group(1);
            }
            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(
                    client,
                    new LdapGroupProviderConfig()
                        .setGroupBaseDistinguishedName(group.getDistinguishedName())
                        .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                        .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                        .setBindPassword("admin")
                        .setGroupSearchFilter("member=${USER}")
                        .setUserBindSearchPatterns("uid=${USER}"),
                    new LdapUtil());

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 1);
            assertEquals(ldapGroupProvider.getGroups("alice"), Collections.singleton(groupName));
        }
    }

    @Test
    public void testMultipleGroups()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group1 = openLdapServer.createGroup(organization);
                DisposableSubContext group2 = openLdapServer.createGroup(organization);
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            openLdapServer.addUserToGroup(ignored, group1);
            openLdapServer.addUserToGroup(ignored, group2);

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(
                    client,
                    new LdapGroupProviderConfig()
                        .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                        .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                        .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                        .setBindPassword("admin")
                        .setGroupSearchFilter("member=${USER}")
                        .setUserBindSearchPatterns("uid=${USER}"),
                    new LdapUtil());

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 2);
        }
    }

    @Test
    public void testAllowUserNotExist()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(
                    client,
                    new LdapGroupProviderConfig()
                        .setAllowUserNotExist(true)
                        .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                        .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                        .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                        .setBindPassword("admin")
                        .setGroupSearchFilter("member=${USER}")
                        .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName()),
                    new LdapUtil());

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 0);
        }
    }
}
