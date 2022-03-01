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
package io.trino.cli;

import com.google.common.net.HostAndPort;
import io.trino.client.ClientSession;
import io.trino.client.OkHttpUtil;
import io.trino.client.StatementClient;
import io.trino.client.auth.external.CompositeRedirectHandler;
import io.trino.client.auth.external.ExternalAuthenticator;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import io.trino.client.auth.external.HttpTokenPoller;
import io.trino.client.auth.external.KnownToken;
import io.trino.client.auth.external.RedirectHandler;
import io.trino.client.auth.external.TokenPoller;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;

import java.io.Closeable;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.ClientSession.stripTransactionId;
import static io.trino.client.OkHttpUtil.basicAuth;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupHttpProxy;
import static io.trino.client.OkHttpUtil.setupKerberos;
import static io.trino.client.OkHttpUtil.setupSocksProxy;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.OkHttpUtil.setupTimeouts;
import static io.trino.client.OkHttpUtil.tokenAuth;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final Consumer<OkHttpClient.Builder> sslSetup;

    public QueryRunner(
            ClientSession session,
            boolean debug,
            HttpLoggingInterceptor.Level networkLogging,
            Optional<HostAndPort> socksProxy,
            Optional<HostAndPort> httpProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> keystoreType,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> truststoreType,
            boolean insecureSsl,
            Optional<String> accessToken,
            Optional<String> user,
            Optional<String> password,
            Optional<String> kerberosPrincipal,
            Optional<String> krb5ServicePrincipalPattern,
            Optional<String> kerberosRemoteServiceName,
            Optional<String> kerberosConfigPath,
            Optional<String> kerberosKeytabPath,
            Optional<String> kerberosCredentialCachePath,
            boolean kerberosUseCanonicalHostname,
            boolean delegatedKerberos,
            boolean externalAuthentication,
            List<ExternalRedirectStrategy> externalRedirectHandlers,
            int externalAccessTokenRefreshInterval)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.debug = debug;

        if (insecureSsl) {
            this.sslSetup = OkHttpUtil::setupInsecureSsl;
        }
        else {
            this.sslSetup = builder -> setupSsl(builder, keystorePath, keystorePassword, keystoreType, truststorePath, truststorePassword, truststoreType);
        }

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        setupTimeouts(builder, 30, SECONDS);
        setupCookieJar(builder);
        setupSocksProxy(builder, socksProxy);
        setupHttpProxy(builder, httpProxy);
        setupBasicAuth(builder, session, user, password);
        setupTokenAuth(builder, session, accessToken);
        setupExternalAuth(builder, session, externalAuthentication, externalRedirectHandlers, externalAccessTokenRefreshInterval, sslSetup);

        builder.addNetworkInterceptor(new HttpLoggingInterceptor(System.err::println).setLevel(networkLogging));

        if (kerberosRemoteServiceName.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using Kerberos requires HTTPS to be enabled");
            setupKerberos(
                    builder,
                    krb5ServicePrincipalPattern.get(),
                    kerberosRemoteServiceName.get(),
                    kerberosUseCanonicalHostname,
                    kerberosPrincipal,
                    kerberosConfigPath.map(File::new),
                    kerberosKeytabPath.map(File::new),
                    kerberosCredentialCachePath.map(File::new),
                    delegatedKerberos);
        }

        this.httpClient = builder.build();
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(requireNonNull(session, "session is null"));
    }

    public boolean isDebug()
    {
        return debug;
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(session.get(), query), debug);
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        OkHttpClient.Builder builder = httpClient.newBuilder();
        sslSetup.accept(builder);
        OkHttpClient client = builder.build();

        return newStatementClient(client, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private static void setupBasicAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> user,
            Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using username/password requires HTTPS to be enabled");
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }

    private static void setupExternalAuth(
            OkHttpClient.Builder builder,
            ClientSession session,
            boolean enabled,
            List<ExternalRedirectStrategy> externalRedirectHandlers,
            int externalAccessTokenRefreshInterval,
            Consumer<OkHttpClient.Builder> sslSetup)
    {
        if (!enabled) {
            return;
        }
        checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                "Authentication using externalAuthentication requires HTTPS to be enabled");

        RedirectHandler redirectHandler = new CompositeRedirectHandler(externalRedirectHandlers);

        TokenPoller poller = new HttpTokenPoller(builder.build(), sslSetup);

        ExternalAuthenticator authenticator = new ExternalAuthenticator(
                redirectHandler,
                poller,
                KnownToken.local(),
                Duration.ofMinutes(10),
                externalAccessTokenRefreshInterval);

        builder.authenticator(authenticator);
        builder.addInterceptor(authenticator);
    }

    private static void setupTokenAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> accessToken)
    {
        if (accessToken.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using an access token requires HTTPS to be enabled");
            clientBuilder.addInterceptor(tokenAuth(accessToken.get()));
        }
    }
}
