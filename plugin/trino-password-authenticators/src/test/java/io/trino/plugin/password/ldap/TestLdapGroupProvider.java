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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.naming.InvalidNameException;
import javax.naming.NamingException;

import java.util.Collections;
import java.util.Set;

import static io.trino.plugin.password.ldap.LdapGroupProvider.extractGroupName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestLdapGroupProvider
{
    private Closer closer;

    /* LDAP configured with group member referential integrity enabled - RFC2307bis e.g. FreeIPA, Augmented Active Directory */
    private TestingOpenLdapServer openLdapServer;
    /* LDAP not configured with group member referential integrity. Active Directory schema objects are available. */
    private TestingOpenLdapServerAD openLdapServerAD;
    private LdapClient client;
    private LdapClient clientAD;

    @BeforeClass
    public void setup()
            throws Exception
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        openLdapServer = closer.register(new TestingOpenLdapServer(network));
        openLdapServerAD = closer.register(new TestingOpenLdapServerAD(network));
        openLdapServer.start();
        openLdapServerAD.start();

        client = new JdkLdapClient(new LdapClientConfig().setLdapUrl(openLdapServer.getLdapUrl()));
        clientAD = new JdkLdapClient(new LdapClientConfig().setLdapUrl(openLdapServerAD.getLdapUrl()));
    }

    @Test
    public void testGetGroups()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group = openLdapServer.createGroup(organization);
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            openLdapServer.addUserToGroup(ignored, group);
            String groupName = extractGroupName(group.getDistinguishedName(), "cn", "memberOf");

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(client,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName(group.getDistinguishedName())
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

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

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(client,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 2);
        }
    }

    @Test
    public void testAllowUserNotExist()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(client,
                    new LdapGroupProviderConfig()
                            .setAllowUserNotExist(true)
                            .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName()));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 0);
        }
    }

    @Test
    public void testExtractGroupName()
            throws NamingException
    {
        assertThatThrownBy(() -> LdapGroupProvider.extractGroupName("alice", "member", null))
                .isInstanceOf(InvalidNameException.class)
                .hasMessageMatching("Invalid name: alice");
        assertThatThrownBy(() -> LdapGroupProvider.extractGroupName("cn=admin,cn=groups,dc=trino,dc=testldap,dc=com", "foo", null))
                .isInstanceOf(NamingException.class)
                .hasMessageStartingWith("ExtractGroupName - Unable to find a group name");
        assertEquals(LdapGroupProvider.extractGroupName("alice", null, "memberOf"), "alice");
        assertEquals(LdapGroupProvider.extractGroupName("cn=admin,cn=groups,dc=trino,dc=testldap,dc=com", "cn", null), "admin");
    }

    @Test
    public void testExtractGroupNameFromUser()
            throws NamingException
    {
        Set<String> userGroups = ImmutableSet.of("cn=admin,cn=groups,dc=trino,dc=testldap,dc=com", "cn=admins,cn=groups,dc=trino,dc=testldap,dc=com");
        Set<String> groupNames = ImmutableSet.of("admin", "admins");
        assertEquals(LdapGroupProvider.extractGroupNameFromUser(userGroups, "cn", null), groupNames);
    }

    @Test
    public void testGroupProviderAttributes()
    {
        assertThatThrownBy(() -> {
            DisposableSubContext organization = openLdapServer.createOrganization();
            DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass");
            new LdapGroupProvider(client, new LdapGroupProviderConfig()
                    .setAllowUserNotExist(true)
                    .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                    .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                    .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                    .setGroupNameAttribute("cn")
                    .setBindPassword("admin")
                    .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName()));
        })
                .isInstanceOf(LdapGroupMembershipException.class)
                .hasMessageMatching("both ldap.group-membership-attribute and " +
                        "ldap.group-user-membership-attribute cannot be empty");

        assertThatThrownBy(() -> {
            DisposableSubContext organization = openLdapServer.createOrganization();
            DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass");
            new LdapGroupProvider(client, new LdapGroupProviderConfig()
                    .setAllowUserNotExist(true)
                    .setGroupBaseDistinguishedName(organization.getDistinguishedName())
                    .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                    .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                    .setGroupMembershipAttribute("member")
                    .setBindPassword("admin")
                    .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName()));
        })
                .isInstanceOf(LdapGroupMembershipException.class)
                .hasMessageMatching("ldap.group-membership-attribute is set "
                        + "ldap.group-name-attribute cannot be empty");
    }

    @Test
    public void testGetGroupsAD()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext group = openLdapServerAD.createGroup(organization);
                DisposableSubContext ignored = openLdapServerAD.createUser(organization, "alice", "alice-pass")) {
            openLdapServerAD.addUserToGroup(ignored, group);
            String groupName = extractGroupName(group.getDistinguishedName(), "cn", "memberOf");

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName(group.getDistinguishedName())
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 1);
            assertEquals(ldapGroupProvider.getGroups("alice"), Collections.singleton(groupName));
        }
    }

    @Test
    public void testGetMultipleGroupsAD()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext group1 = openLdapServerAD.createGroup(organization);
                DisposableSubContext group2 = openLdapServerAD.createGroup(organization);
                DisposableSubContext ignored = openLdapServerAD.createUser(organization, "alice", "alice-pass")) {
            openLdapServerAD.addUserToGroup(ignored, group1);
            openLdapServerAD.addUserToGroup(ignored, group2);
            String groupName1 = extractGroupName(group1.getDistinguishedName(), "cn", "memberOf");
            String groupName2 = extractGroupName(group2.getDistinguishedName(), "cn", "memberOf");

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName("dc=trino,dc=testldap,dc=com")
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 2);
            assertEquals(ldapGroupProvider.getGroups("alice"), ImmutableSet.of(groupName1, groupName2));
        }
    }

    @Test
    public void testGetGroupsADFromGroupMember()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext ignored = openLdapServerAD.createUser(organization, "alice", "alice-pass");
                DisposableSubContext group = openLdapServerAD.createGroup(organization, "member", ignored.getDistinguishedName())) {
            String groupName = extractGroupName(group.getDistinguishedName(), "cn", null);

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName(group.getDistinguishedName())
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 1);
            assertEquals(ldapGroupProvider.getGroups("alice"), Collections.singleton(groupName));
        }
    }

    @Test
    public void testGetMultipleGroupsADFromGroupMember()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext ignored = openLdapServerAD.createUser(organization, "alice", "alice-pass");
                DisposableSubContext group1 = openLdapServerAD.createGroup(organization, "member", ignored.getDistinguishedName());
                DisposableSubContext group2 = openLdapServerAD.createGroup(organization, "member", ignored.getDistinguishedName())) {
            String groupName1 = extractGroupName(group1.getDistinguishedName(), "cn", null);
            String groupName2 = extractGroupName(group2.getDistinguishedName(), "cn", null);

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setGroupBaseDistinguishedName("dc=trino,dc=testldap,dc=com")
                            .setUserBaseDistinguishedName(ignored.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setGroupNameAttribute("cn")
                            .setBindPassword("admin")
                            .setGroupMembershipAttribute("member")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 2);
            assertEquals(ldapGroupProvider.getGroups("alice"), ImmutableSet.of(groupName1, groupName2));
        }
    }

    @Test
    public void testGetGroupsADFromUserMemberOfNoGroup()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext user = openLdapServerAD.createUser(
                        organization, "alice", "alice-pass", ImmutableSet.of("admins"))) {
            String groupName = extractGroupName("admins", null, "memberOf");

            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setUserBaseDistinguishedName(user.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setBindPassword("admin")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 1);
            assertEquals(ldapGroupProvider.getGroups("alice"), Collections.singleton(groupName));
        }
    }

    @Test
    public void testGetMultipleGroupsADFromUserMemberOfNoGroup()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServerAD.createOrganization();
                DisposableSubContext user = openLdapServerAD.createUser(
                        organization, "alice", "alice-pass", ImmutableSet.of("admins", "users"))) {
            LdapGroupProvider ldapGroupProvider = new LdapGroupProvider(clientAD,
                    new LdapGroupProviderConfig()
                            .setUserBaseDistinguishedName(user.getDistinguishedName())
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setBindPassword("admin")
                            .setGroupUserMembershipAttribute("memberOf")
                            .setUserBindSearchPatterns("uid=${USER}"));

            assertEquals(ldapGroupProvider.getGroups("alice").size(), 2);
            assertEquals(ldapGroupProvider.getGroups("alice"), ImmutableSet.of("admins", "users"));
        }
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        closer.close();
        closer = null;
        openLdapServer = null;
        openLdapServerAD = null;
        client = null;
    }
}
