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
import io.trino.plugin.base.ldap.LdapClientConfig;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import java.util.Properties;
import java.util.Set;

import static io.trino.testing.containers.ldap.LdapUtil.addLdapDefinition;
import static io.trino.testing.containers.ldap.LdapUtil.buildLdapGroupObject;
import static io.trino.testing.containers.ldap.LdapUtil.buildLdapUserObject;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end reproduction of the unpaged-LDAP-search bug against a large directory.
 * <p>
 * The directory is loaded with {@value USER_COUNT} users and {@value GROUP_COUNT} groups, and an OpenLDAP
 * per-identity limit of {@value SIZE_LIMIT} entries is applied to authenticated (non-admin) binds (with
 * {@code size.prtotal=unlimited} so paged searches may exceed it). Binding as a regular user then exposes the
 * two acceptance criteria:
 * <ul>
 *     <li><b>Problem:</b> a plain, non-paged search — exactly what the pre-patch client issued — is truncated
 *         by the server and fails with {@link SizeLimitExceededException}, so entries beyond the first page are
 *         never seen.</li>
 *     <li><b>Fix:</b> the patched {@link JdkLdapClient}, which issues an RFC 2696 paged search, walks every page
 *         and returns the full result set for both users and groups.</li>
 * </ul>
 * The admin/rootdn bind bypasses server limits, so the search must run as a regular user to observe truncation.
 */
@TestInstance(PER_CLASS)
public class TestLdapLargeDirectoryPagedSearch
{
    private static final int USER_COUNT = 5000;
    private static final int GROUP_COUNT = 3000;
    private static final int SIZE_LIMIT = 500;

    private static final String ADMIN_DISTINGUISHED_NAME = "cn=admin,dc=trino,dc=testldap,dc=com";
    private static final String ADMIN_PASSWORD = "admin";

    private final Closer closer;
    private final TestingOpenLdapServer openLdapServer;
    private final String organizationDistinguishedName;
    private final String regularUserDistinguishedName;
    private final String regularUserPassword;

    public TestLdapLargeDirectoryPagedSearch()
            throws Exception
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        openLdapServer = closer.register(new TestingOpenLdapServer(network));
        openLdapServer.start();

        // The organization is intentionally not closed: with thousands of child entries the container teardown
        // is the cheapest way to dispose of it.
        DisposableSubContext organization = openLdapServer.createOrganization();
        organizationDistinguishedName = organization.getDistinguishedName();

        regularUserDistinguishedName = format("uid=paged_user_0,%s", organizationDistinguishedName);
        regularUserPassword = "password-0";

        // The default LMDB map size cannot hold thousands of entries; raise it before bulk-loading.
        openLdapServer.setDatabaseMaxSizeBytes(1_073_741_824L);
        loadDirectory();
        openLdapServer.limitAuthenticatedUserSearchSize(SIZE_LIMIT);
    }

    @AfterAll
    public void close()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testNonPagedSearchIsTruncated()
    {
        // Reproduces the pre-patch behaviour: a single, non-paged search as a regular user runs into the server
        // size limit and fails outright instead of returning the matching users beyond the first page.
        assertThatThrownBy(() -> nonPagedSearch("(objectClass=inetOrgPerson)"))
                .isInstanceOf(SizeLimitExceededException.class);
    }

    @Test
    public void testPagedSearchReturnsAllUsers()
            throws Exception
    {
        assertThat(readAllDistinguishedNames("(objectClass=inetOrgPerson)"))
                .as("paged search must return every user past the server size limit")
                .hasSize(USER_COUNT);
    }

    @Test
    public void testPagedSearchReturnsAllGroups()
            throws Exception
    {
        assertThat(readAllDistinguishedNames("(objectClass=groupOfNames)"))
                .as("paged search must return every group past the server size limit")
                .hasSize(GROUP_COUNT);
    }

    /**
     * Walks every page of a paged search as a regular (non-admin) user, mirroring the full-enumeration path the
     * group providers use. {@code size.prtotal=unlimited} lets the paged search read past the per-identity size
     * limit that truncates the non-paged search in {@link #testNonPagedSearchIsTruncated()}.
     */
    private Set<String> readAllDistinguishedNames(String filter)
            throws NamingException
    {
        JdkLdapClient client = new JdkLdapClient(new LdapClientConfig()
                .setLdapUrl(openLdapServer.getLdapUrl()));
        return client.executeLdapQuery(
                regularUserDistinguishedName,
                regularUserPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(organizationDistinguishedName)
                        .withSearchFilter(filter)
                        .build(),
                searchResults -> {
                    ImmutableSet.Builder<String> distinguishedNames = ImmutableSet.builder();
                    while (searchResults.hasMore()) {
                        distinguishedNames.add(searchResults.next().getNameInNamespace());
                    }
                    return distinguishedNames.build();
                });
    }

    private void loadDirectory()
            throws NamingException
    {
        DirContext context = adminContext();
        try {
            for (int i = 0; i < USER_COUNT; i++) {
                addLdapDefinition(buildLdapUserObject(organizationDistinguishedName, "paged_user_" + i, "password-" + i), context);
            }
            for (int i = 0; i < GROUP_COUNT; i++) {
                addLdapDefinition(buildLdapGroupObject(organizationDistinguishedName, "paged_group_" + i), context);
            }
        }
        finally {
            context.close();
        }
    }

    private void nonPagedSearch(String filter)
            throws NamingException
    {
        Properties environment = new Properties();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, openLdapServer.getLdapUrl());
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.SECURITY_PRINCIPAL, regularUserDistinguishedName);
        environment.put(Context.SECURITY_CREDENTIALS, regularUserPassword);

        DirContext context = new InitialDirContext(environment);
        try {
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            NamingEnumeration<SearchResult> results = context.search(organizationDistinguishedName, filter, searchControls);
            while (results.hasMore()) {
                results.next();
            }
        }
        finally {
            context.close();
        }
    }

    private DirContext adminContext()
            throws NamingException
    {
        Properties environment = new Properties();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, openLdapServer.getLdapUrl());
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.SECURITY_PRINCIPAL, ADMIN_DISTINGUISHED_NAME);
        environment.put(Context.SECURITY_CREDENTIALS, ADMIN_PASSWORD);
        return new InitialDirContext(environment);
    }
}
