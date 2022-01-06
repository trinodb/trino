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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.security.pem.PemReader;
import io.trino.spi.security.AccessDeniedException;

import javax.inject.Inject;
import javax.naming.AuthenticationException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.password.jndi.JndiUtils.createDirContext;
import static java.util.Objects.requireNonNull;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.REFERRAL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;

public class JdkLdapAuthenticatorClient
        implements LdapAuthenticatorClient
{
    private static final Logger log = Logger.get(JdkLdapAuthenticatorClient.class);

    private final Map<String, String> basicEnvironment;
    private final Optional<SSLContext> sslContext;

    @Inject
    public JdkLdapAuthenticatorClient(LdapConfig ldapConfig)
    {
        String ldapUrl = requireNonNull(ldapConfig.getLdapUrl(), "ldapUrl is null");
        if (ldapUrl.startsWith("ldap://")) {
            log.warn("Passwords will be sent in the clear to the LDAP server. Please consider using SSL to connect.");
        }

        this.basicEnvironment = ImmutableMap.<String, String>builder()
                .put(INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(PROVIDER_URL, ldapUrl)
                .put(REFERRAL, ldapConfig.isIgnoreReferrals() ? "ignore" : "follow")
                .build();

        if (ldapConfig.isIgnoreCertificate()) {
            this.sslContext = Optional.of(createIgnoreCertificateSslContext());
        }
        else {
            this.sslContext = Optional.ofNullable(ldapConfig.getTrustCertificate())
                    .map(JdkLdapAuthenticatorClient::createSslContext);
        }
    }

    @Override
    public void validatePassword(String userDistinguishedName, String password)
            throws NamingException
    {
        createUserDirContext(userDistinguishedName, password).close();
    }

    @Override
    public boolean isGroupMember(String searchBase, String groupSearch, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        try (CloseableContext context = createUserDirContext(contextUserDistinguishedName, contextPassword);
                CloseableSearchResults search = searchContext(searchBase, groupSearch, context)) {
            return search.hasMore();
        }
    }

    @Override
    public Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        try (CloseableContext context = createUserDirContext(contextUserDistinguishedName, contextPassword);
                CloseableSearchResults search = searchContext(searchBase, searchFilter, context)) {
            ImmutableSet.Builder<String> distinguishedNames = ImmutableSet.builder();
            while (search.hasMore()) {
                distinguishedNames.add(search.next().getNameInNamespace());
            }
            return distinguishedNames.build();
        }
    }

    private static CloseableSearchResults searchContext(String searchBase, String searchFilter, CloseableContext context)
            throws NamingException
    {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        return new CloseableSearchResults(context.search(searchBase, searchFilter, searchControls));
    }

    private CloseableContext createUserDirContext(String userDistinguishedName, String password)
            throws NamingException
    {
        Map<String, String> environment = createEnvironment(userDistinguishedName, password);
        try {
            // This is the actual Authentication piece. Will throw javax.naming.AuthenticationException
            // if the users password is not correct. Other exceptions may include IO (server not found) etc.
            DirContext context = createDirContext(environment);
            log.debug("Password validation successful for user DN [%s]", userDistinguishedName);
            return new CloseableContext(context);
        }
        catch (AuthenticationException e) {
            log.debug("Password validation failed for user DN [%s]: %s", userDistinguishedName, e.getMessage());
            throw new AccessDeniedException("Invalid credentials");
        }
    }

    private Map<String, String> createEnvironment(String userDistinguishedName, String password)
    {
        ImmutableMap.Builder<String, String> environment = ImmutableMap.<String, String>builder()
                .putAll(basicEnvironment)
                .put(SECURITY_AUTHENTICATION, "simple")
                .put(SECURITY_PRINCIPAL, userDistinguishedName)
                .put(SECURITY_CREDENTIALS, password);

        sslContext.ifPresent(context -> {
            LdapSslSocketFactory.setSslContextForCurrentThread(context);

            // see https://docs.oracle.com/javase/jndi/tutorial/ldap/security/ssl.html
            environment.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());
        });

        return environment.build();
    }

    private static SSLContext createIgnoreCertificateSslContext()
    {
        try {
            TrustManager[] trustManagers = {
                new X509TrustManager()
                {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType)
                    {
                        throw new UnsupportedOperationException("checkClientTrusted should not be called");
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType)
                    {
                        // skip validation of server certificate
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers()
                    {
                        return new X509Certificate[0];
                    }
                }
            };
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustManagers, null);
            return sslContext;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static SSLContext createSslContext(File trustCertificate)
    {
        try {
            KeyStore trustStore = PemReader.loadTrustStore(trustCertificate);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustManagers, null);
            return sslContext;
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CloseableContext
            implements AutoCloseable
    {
        private final DirContext context;

        public CloseableContext(DirContext context)
        {
            this.context = requireNonNull(context, "context is null");
        }

        public NamingEnumeration<SearchResult> search(String name, String filter, SearchControls searchControls)
                throws NamingException
        {
            return context.search(name, filter, searchControls);
        }

        @Override
        public void close()
                throws NamingException
        {
            context.close();
        }
    }

    private static class CloseableSearchResults
            implements AutoCloseable
    {
        private final NamingEnumeration<SearchResult> searchResults;

        public CloseableSearchResults(NamingEnumeration<SearchResult> searchResults)
        {
            this.searchResults = requireNonNull(searchResults, "searchResults is null");
        }

        public SearchResult next()
                throws NamingException
        {
            return searchResults.next();
        }

        public boolean hasMore()
                throws NamingException
        {
            return searchResults.hasMore();
        }

        @Override
        public void close()
                throws NamingException
        {
            searchResults.close();
        }
    }
}
