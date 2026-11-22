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
package org.apache.ranger.audit.destination;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.InMemoryJAASConfiguration;
import org.apache.ranger.audit.utils.KerberosAction;
import org.apache.ranger.audit.utils.KerberosJAASConfigUser;
import org.apache.ranger.audit.utils.KerberosUser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SocketFactoryRegistryProvider;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Replacement for the class of the same name shipped in {@code ranger-audit-dest-solr}.
 * <p>
 * The upstream implementation configures SPNEGO through SolrJ's {@code Krb5HttpClientBuilder},
 * which cannot be loaded without Jetty 9-11 on the classpath, even though audit events
 * are sent with Apache HttpClient and Jetty is never used. This implementation configures
 * SPNEGO directly on the Apache HttpClient used by SolrJ, so that Jetty is not needed at all.
 * <p>
 * The class has to keep the upstream name, because Ranger instantiates the Solr audit
 * destination by its fully qualified class name.
 */
// TODO: remove once Ranger no longer requires an unsupported Jetty version for the Solr audit destination
public class SolrAuditDestination
        extends AuditDestination
{
    private static final Logger log = Logger.get(SolrAuditDestination.class);

    public static final String PROP_SOLR_URLS = "urls";
    public static final String PROP_SOLR_ZK = "zookeepers";
    public static final String PROP_SOLR_COLLECTION = "collection";
    public static final String PROP_SOLR_FORCE_USE_INMEMORY_JAAS_CONFIG = "force.use.inmemory.jaas.config";

    public static final String DEFAULT_COLLECTION_NAME = "ranger_audits";
    public static final String PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

    private static final String USE_SUBJECT_CREDENTIALS_ONLY = "javax.security.auth.useSubjectCredsOnly";
    private static final String JAAS_APPLICATION_NAME = "solr.kerberos.jaas.appname";
    private static final int CONNECTION_TIMEOUT_MILLIS = 1000;

    // SPNEGO authentication consumes the request body, so it has to be repeatable
    private static final HttpRequestInterceptor BUFFERED_ENTITY_INTERCEPTOR = (request, _) -> {
        if (request instanceof HttpEntityEnclosingRequest enclosingRequest && enclosingRequest.getEntity() != null) {
            enclosingRequest.setEntity(new BufferedHttpEntity(enclosingRequest.getEntity()));
        }
    };

    private volatile SolrClient solrClient;
    private volatile KerberosUser kerberosUser;

    @Override
    public void init(Properties props, String propPrefix)
    {
        super.init(props, propPrefix);
        initializeJaasConfiguration();
        connect();
    }

    @Override
    public void stop()
    {
        logStatus();

        SolrClient solrClient = this.solrClient;
        this.solrClient = null;
        if (solrClient != null) {
            try {
                solrClient.close();
            }
            catch (IOException e) {
                log.error(e, "Error while closing Solr client");
            }
        }

        KerberosUser kerberosUser = this.kerberosUser;
        this.kerberosUser = null;
        if (kerberosUser != null) {
            try {
                kerberosUser.logout();
            }
            catch (LoginException e) {
                log.error(e, "Error logging out keytab user");
            }
        }
    }

    @Override
    public boolean log(Collection<AuditEventBase> events)
    {
        try {
            logStatusIfRequired();
            addTotalCount(events.size());

            if (solrClient == null) {
                connect();
            }
            SolrClient solrClient = this.solrClient;
            if (solrClient == null) {
                // Solr is still not available, let the caller retry
                addDeferredCount(events.size());
                return false;
            }

            List<SolrInputDocument> documents = new ArrayList<>(events.size());
            for (AuditEventBase event : events) {
                documents.add(toSolrDocument((AuthzAuditEvent) event));
            }

            try {
                UpdateResponse response = addDocuments(solrClient, documents);
                if (response.getStatus() != 0) {
                    addFailedCount(events.size());
                    logFailedEvent(events, response.toString());
                    return false;
                }
                addSuccessCount(events.size());
                return true;
            }
            catch (SolrException e) {
                addFailedCount(events.size());
                logFailedEvent(events, e);
                return false;
            }
        }
        catch (Throwable t) {
            addDeferredCount(events.size());
            logError("Error sending message to Solr", t);
            return false;
        }
    }

    @Override
    public void flush() {}

    public boolean isAsync()
    {
        return true;
    }

    private synchronized void connect()
    {
        if (solrClient != null) {
            return;
        }

        String urls = normalize(MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SOLR_URLS));
        String zkHosts = normalize(MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SOLR_ZK));
        String collection = normalize(MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SOLR_COLLECTION));
        String collectionName = collection == null ? DEFAULT_COLLECTION_NAME : collection;

        log.info("Solr zkHosts=%s, solrURLs=%s, collectionName=%s", zkHosts, urls, collectionName);
        if (zkHosts == null && urls == null) {
            return;
        }

        try {
            // SolrJ keeps the HTTP client configuration in static fields. It is shared by all Solr clients
            // created in the plugin class loader, and it is not visible outside of it.
            synchronized (SolrAuditDestination.class) {
                configureSsl(props);
                configureSpnego();

                if (zkHosts != null) {
                    List<String> hosts = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(zkHosts);
                    CloudSolrClient client = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<CloudSolrClient>) () ->
                            new CloudSolrClient.Builder(hosts, Optional.empty()).build());
                    client.setDefaultCollection(collectionName);
                    solrClient = client;
                }
                else {
                    String[] solrUrls = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(urls).toArray(String[]::new);
                    solrClient = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<LBHttpSolrClient>) () ->
                            new LBHttpSolrClient.Builder()
                                    .withBaseSolrUrls(solrUrls)
                                    .withConnectionTimeout(CONNECTION_TIMEOUT_MILLIS)
                                    .build());
                }
            }
        }
        catch (Throwable t) {
            log.error(t, "Can't connect to Solr server. ZooKeepers=%s, URLs=%s", zkHosts, urls);
        }
    }

    private UpdateResponse addDocuments(SolrClient solrClient, Collection<SolrInputDocument> documents)
            throws Exception
    {
        PrivilegedExceptionAction<UpdateResponse> action = () -> solrClient.add(documents);

        KerberosUser kerberosUser = this.kerberosUser;
        if (kerberosUser == null) {
            return action.run();
        }
        // execute the action as the keytab user
        return new KerberosAction<>(kerberosUser, action, LoggerFactory.getLogger(SolrAuditDestination.class)).execute();
    }

    private void initializeJaasConfiguration()
    {
        try {
            // SolrJ only authenticates with Kerberos when the "java.security.auth.login.config" system property is set.
            // The actual JAAS configuration is read from the Ranger audit configuration by InMemoryJAASConfiguration.
            if (System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null) {
                if (MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_SOLR_FORCE_USE_INMEMORY_JAAS_CONFIG, false)) {
                    System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG, "/dev/null");
                }
                else {
                    log.debug("No client JAAS configuration present in Solr audit configuration, Kerberos authentication is disabled");
                }
            }

            InMemoryJAASConfiguration configuration = InMemoryJAASConfiguration.init(props);
            KerberosUser kerberosUser = new KerberosJAASConfigUser("Client", configuration);
            if (kerberosUser.getPrincipal() != null) {
                this.kerberosUser = kerberosUser;
            }
        }
        catch (Exception e) {
            log.error(e, "Unable to load Solr client JAAS configuration from audit configuration, audit to Kerberized Solr will fail");
        }
    }

    /**
     * Configures SPNEGO authentication for the Apache HttpClient used by SolrJ.
     * Equivalent of {@code Krb5HttpClientBuilder#getBuilder} from SolrJ.
     */
    private static void configureSpnego()
    {
        String loginConfiguration = System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG);
        if (loginConfiguration == null) {
            return;
        }
        log.info("Setting up SPNEGO authentication with config: %s", loginConfiguration);

        // Credentials have to be loaded from the JAAS configuration by the GSS mechanism
        String useSubjectCredentialsOnly = System.getProperty(USE_SUBJECT_CREDENTIALS_ONLY);
        if (useSubjectCredentialsOnly == null) {
            System.setProperty(USE_SUBJECT_CREDENTIALS_ONLY, "false");
        }
        else if (!useSubjectCredentialsOnly.equalsIgnoreCase("false")) {
            log.warn("System property %s is set to %s, SPNEGO authentication may not be successful", USE_SUBJECT_CREDENTIALS_ONLY, useSubjectCredentialsOnly);
        }

        try {
            Configuration configuration = Configuration.getConfiguration();
            if (!(configuration instanceof SolrJaasConfiguration)) {
                Configuration.setConfiguration(new SolrJaasConfiguration(configuration));
            }
        }
        catch (SecurityException e) {
            log.warn(e, "Unable to load JAAS configuration from %s, SPNEGO authentication may not be successful", loginConfiguration);
        }

        SolrHttpClientBuilder builder = HttpClientUtil.getHttpClientBuilder();

        // Enable only the SPNEGO authentication scheme
        Registry<AuthSchemeProvider> authSchemes = RegistryBuilder.<AuthSchemeProvider>create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false))
                .build();
        builder.setAuthSchemeRegistryProvider(() -> authSchemes);

        HttpClientUtil.setCookiePolicy(SolrPortAwareCookieSpecFactory.POLICY_NAME);
        Registry<CookieSpecProvider> cookieSpecs = RegistryBuilder.<CookieSpecProvider>create()
                .register(SolrPortAwareCookieSpecFactory.POLICY_NAME, new SolrPortAwareCookieSpecFactory())
                .build();
        builder.setCookieSpecRegistryProvider(() -> cookieSpecs);

        builder.setDefaultCredentialsProvider(() -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, JaasCredentials.INSTANCE);
            return credentialsProvider;
        });

        HttpClientUtil.removeRequestInterceptor(BUFFERED_ENTITY_INTERCEPTOR);
        HttpClientUtil.addRequestInterceptor(BUFFERED_ENTITY_INTERCEPTOR);
        HttpClientUtil.setHttpClientBuilder(builder);
    }

    /**
     * Makes SolrJ use the key store and the trust store from the Ranger configuration.
     * When none is configured, SolrJ uses the default {@link SSLContext} of the JVM.
     */
    private static void configureSsl(Properties props)
            throws GeneralSecurityException, IOException
    {
        KeyManager[] keyManagers = loadKeyManagers(props);
        TrustManager[] trustManagers = loadTrustManagers(props);
        if (keyManagers == null && trustManagers == null) {
            return;
        }

        SSLContext sslContext = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);
        sslContext.init(keyManagers, trustManagers, new SecureRandom());

        boolean checkPeerName = Boolean.parseBoolean(System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "true"));
        HostnameVerifier hostnameVerifier = checkPeerName ? SSLConnectionSocketFactory.getDefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", new SSLConnectionSocketFactory(sslContext, hostnameVerifier))
                .build();
        HttpClientUtil.setSocketFactoryRegistryProvider(new SocketFactoryRegistryProvider()
        {
            @Override
            public Registry<ConnectionSocketFactory> getSocketFactoryRegistry()
            {
                return registry;
            }
        });
    }

    private static KeyManager[] loadKeyManagers(Properties props)
            throws GeneralSecurityException, IOException
    {
        String file = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE);
        String password = MiscUtil.getCredentialString(MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL), RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS);
        if (isNullOrEmpty(file) || isNullOrEmpty(password)) {
            return null;
        }

        String type = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE);
        KeyStore keyStore = loadKeyStore(file, isNullOrEmpty(type) ? RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT : type, password);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RANGER_SSL_KEYMANAGER_ALGO_TYPE);
        keyManagerFactory.init(keyStore, password.toCharArray());
        return keyManagerFactory.getKeyManagers();
    }

    private static TrustManager[] loadTrustManagers(Properties props)
            throws GeneralSecurityException, IOException
    {
        String file = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE);
        String password = MiscUtil.getCredentialString(MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL), RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS);
        if (isNullOrEmpty(file) || isNullOrEmpty(password)) {
            return null;
        }

        String type = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE);
        KeyStore trustStore = loadKeyStore(file, isNullOrEmpty(type) ? RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT : type, password);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private static KeyStore loadKeyStore(String file, String type, String password)
            throws GeneralSecurityException, IOException
    {
        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream in = openFile(file)) {
            keyStore.load(in, password.toCharArray());
        }
        return keyStore;
    }

    private static InputStream openFile(String file)
            throws IOException
    {
        Path path = Path.of(file);
        if (Files.exists(path)) {
            return Files.newInputStream(path);
        }
        InputStream in = ClassLoader.getSystemResourceAsStream(file);
        if (in == null) {
            throw new IOException("File not found: " + file);
        }
        return in;
    }

    private static String normalize(String value)
    {
        if (value == null) {
            return null;
        }
        value = value.trim();
        if (value.isEmpty() || value.equalsIgnoreCase("NONE")) {
            return null;
        }
        return value;
    }

    private static SolrInputDocument toSolrDocument(AuthzAuditEvent event)
    {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", event.getEventId());
        document.addField("access", event.getAccessType());
        document.addField("enforcer", event.getAclEnforcer());
        document.addField("agent", event.getAgentId());
        document.addField("repo", event.getRepositoryName());
        document.addField("sess", event.getSessionId());
        document.addField("reqUser", event.getUser());
        document.addField("reqData", event.getRequestData());
        document.addField("resource", event.getResourcePath());
        document.addField("cliIP", event.getClientIP());
        document.addField("logType", event.getLogType());
        document.addField("result", event.getAccessResult());
        document.addField("policy", event.getPolicyId());
        document.addField("repoType", event.getRepositoryType());
        document.addField("resType", event.getResourceType());
        document.addField("reason", event.getResultReason());
        document.addField("action", event.getAction());
        document.addField("evtTime", event.getEventTime());
        document.addField("seq_num", event.getSeqNum());
        document.setField("event_count", event.getEventCount());
        document.setField("event_dur_ms", event.getEventDurationMS());
        document.setField("tags", event.getTags());
        document.setField("cluster", event.getClusterName());
        document.setField("zoneName", event.getZoneName());
        document.setField("agentHost", event.getAgentHostname());
        document.setField("policyVersion", event.getPolicyVersion());
        return document;
    }

    /**
     * Credentials are obtained from the JAAS configuration by the GSS mechanism.
     */
    private enum JaasCredentials
            implements Credentials
    {
        INSTANCE;

        @Override
        public Principal getUserPrincipal()
        {
            return null;
        }

        @Override
        public String getPassword()
        {
            return null;
        }
    }

    /**
     * Makes the GSS initiator use the JAAS entry of the Solr client.
     */
    private static class SolrJaasConfiguration
            extends Configuration
    {
        private static final Set<String> INITIATE_APPLICATION_NAMES = ImmutableSet.of("com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate");

        private final Configuration delegate;

        public SolrJaasConfiguration(Configuration delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name)
        {
            if (INITIATE_APPLICATION_NAMES.contains(name)) {
                return delegate.getAppConfigurationEntry(System.getProperty(JAAS_APPLICATION_NAME, "Client"));
            }
            return delegate.getAppConfigurationEntry(name);
        }
    }
}
