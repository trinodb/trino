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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.testing.TestingProperties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static io.trino.plugin.password.jndi.JndiUtils.createDirContext;
import static io.trino.plugin.password.ldap.LdapUtil.MEMBER;
import static io.trino.plugin.password.ldap.LdapUtil.addAttributesToExistingLdapObjects;
import static io.trino.plugin.password.ldap.LdapUtil.addLdapDefinition;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapGroupObject;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapOrganizationObject;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapUserObject;
import static io.trino.plugin.password.ldap.LdapUtil.randomSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingOpenLdapServer
        implements Closeable
{
    private static final String BASE_DISTINGUISED_NAME = "dc=trino,dc=testldap,dc=com";

    private static final int LDAP_PORT = 389;

    private final Closer closer = Closer.create();
    private final GenericContainer<?> openLdapServer;

    public TestingOpenLdapServer()
    {
        openLdapServer = new GenericContainer<>("ghcr.io/trinodb/testing/centos7-oj11-openldap:" + TestingProperties.getDockerImagesVersion())
                .withExposedPorts(LDAP_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy())
                .withStartupTimeout(Duration.ofMinutes(5));

        closer.register(openLdapServer::close);
    }

    public void start()
    {
        openLdapServer.start();
    }

    public String getLdapUrl()
    {
        return format("ldap://%s:%s", openLdapServer.getContainerIpAddress(), openLdapServer.getMappedPort(LDAP_PORT));
    }

    public DisposableSubContext createOrganization()
            throws NamingException
    {
        DirContext context = createContext();
        try {
            return new DisposableSubContext(addLdapDefinition(buildLdapOrganizationObject("organization_" + randomSuffix(), BASE_DISTINGUISED_NAME), context));
        }
        finally {
            context.close();
        }
    }

    public DisposableSubContext createGroup(DisposableSubContext organization)
            throws Exception
    {
        DirContext context = createContext();
        try {
            return new DisposableSubContext(addLdapDefinition(buildLdapGroupObject(organization.getDistinguishedName(), "group_" + randomSuffix()), context));
        }
        finally {
            context.close();
        }
    }

    public DisposableSubContext createUser(DisposableSubContext organization, String userName, String password)
            throws Exception
    {
        DirContext context = createContext();
        try {
            return new DisposableSubContext(addLdapDefinition(buildLdapUserObject(organization.getDistinguishedName(), userName, password), context));
        }
        finally {
            context.close();
        }
    }

    public void addUserToGroup(DisposableSubContext userDistinguishedName, DisposableSubContext groupDistinguishedName)
            throws Exception
    {
        DirContext context = createContext();
        try {
            addAttributesToExistingLdapObjects(
                    groupDistinguishedName.getDistinguishedName(),
                    ImmutableMap.of(MEMBER, ImmutableList.of(userDistinguishedName.getDistinguishedName())),
                    context);
        }
        finally {
            context.close();
        }
    }

    private DirContext createContext()
    {
        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, getLdapUrl())
                .put(Context.SECURITY_AUTHENTICATION, "simple")
                .put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=trino,dc=testldap,dc=com")
                .put(Context.SECURITY_CREDENTIALS, "admin")
                .buildOrThrow();
        try {
            return createDirContext(environment);
        }
        catch (NamingException e) {
            throw new RuntimeException("Connection to LDAP server failed", e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    public class DisposableSubContext
            implements AutoCloseable
    {
        private final String distinguishedName;

        public DisposableSubContext(String distinguishedName)
        {
            this.distinguishedName = requireNonNull(distinguishedName, "distinguishedName is null");
        }

        public String getDistinguishedName()
        {
            return distinguishedName;
        }

        @Override
        public void close()
                throws Exception
        {
            DirContext context = createContext();
            context.destroySubcontext(distinguishedName);
            context.close();
        }
    }
}
