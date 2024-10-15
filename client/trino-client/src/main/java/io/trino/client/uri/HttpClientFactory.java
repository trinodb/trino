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
package io.trino.client.uri;

import io.trino.client.ClientException;
import io.trino.client.DnsResolver;
import io.trino.client.auth.external.CompositeRedirectHandler;
import io.trino.client.auth.external.ExternalAuthenticator;
import io.trino.client.auth.external.HttpTokenPoller;
import io.trino.client.auth.external.RedirectHandler;
import io.trino.client.auth.external.TokenPoller;
import okhttp3.OkHttpClient;

import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.client.KerberosUtil.defaultCredentialCachePath;
import static io.trino.client.OkHttpUtil.basicAuth;
import static io.trino.client.OkHttpUtil.disableHttp2;
import static io.trino.client.OkHttpUtil.setupAlternateHostnameVerification;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupHttpLogging;
import static io.trino.client.OkHttpUtil.setupHttpProxy;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.client.OkHttpUtil.setupKerberos;
import static io.trino.client.OkHttpUtil.setupSocksProxy;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.OkHttpUtil.setupTimeouts;
import static io.trino.client.OkHttpUtil.tokenAuth;
import static io.trino.client.OkHttpUtil.userAgent;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.CA;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.FULL;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.NONE;
import static java.lang.Math.toIntExact;

public class HttpClientFactory
{
    private HttpClientFactory() {}

    public static OkHttpClient.Builder toHttpClientBuilder(TrinoUri uri, String userAgent)
    {
        OkHttpClient.Builder builder = unauthenticatedClientBuilder(uri, userAgent);
        disableHttp2(builder);
        setupCookieJar(builder);

        if (!uri.isUseSecureConnection()) {
            setupInsecureSsl(builder);
        }

        if (uri.hasPassword()) {
            if (!uri.isUseSecureConnection()) {
                throw new RuntimeException("TLS/SSL is required for authentication with username and password");
            }
            builder.addInterceptor(basicAuth(uri.getRequiredUser(), uri.getPassword().orElseThrow(() -> new RuntimeException("Password expected"))));
        }

        if (uri.isUseSecureConnection()) {
            ConnectionProperties.SslVerificationMode sslVerificationMode = uri.getSslVerification();
            if (sslVerificationMode.equals(FULL) || sslVerificationMode.equals(CA)) {
                setupSsl(
                        builder,
                        uri.getSslKeyStorePath(),
                        uri.getSslKeyStorePassword(),
                        uri.getSslKeyStoreType(),
                        uri.getSslUseSystemKeyStore(),
                        uri.getSslTrustStorePath(),
                        uri.getSslTrustStorePassword(),
                        uri.getSslTrustStoreType(),
                        uri.getSslUseSystemTrustStore());
            }
            if (sslVerificationMode.equals(FULL)) {
                uri.getHostnameInCertificate().ifPresent(certHostname ->
                        setupAlternateHostnameVerification(builder, certHostname));
            }

            if (sslVerificationMode.equals(CA)) {
                builder.hostnameVerifier((hostname, session) -> true);
            }

            if (sslVerificationMode.equals(NONE)) {
                setupInsecureSsl(builder);
            }
        }

        if (uri.getKerberosRemoteServiceName().isPresent()) {
            if (!uri.isUseSecureConnection()) {
                throw new RuntimeException("TLS/SSL is required for Kerberos authentication");
            }
            setupKerberos(
                    builder,
                    uri.getRequiredKerberosServicePrincipalPattern(),
                    uri.getRequiredKerberosRemoteServiceName(),
                    uri.getRequiredKerberosUseCanonicalHostname(),
                    uri.getKerberosPrincipal(),
                    uri.getKerberosConfigPath(),
                    uri.getKerberosKeytabPath(),
                    Optional.ofNullable(uri.getKerberosCredentialCachePath()
                            .orElseGet(() -> defaultCredentialCachePath().map(File::new).orElse(null))),
                    uri.getKerberosDelegation(),
                    uri.getKerberosConstrainedDelegation());
        }

        if (uri.getAccessToken().isPresent()) {
            if (!uri.isUseSecureConnection()) {
                throw new RuntimeException("TLS/SSL required for authentication using an access token");
            }
            builder.addInterceptor(tokenAuth(uri.getAccessToken().get()));
        }

        if (uri.isExternalAuthenticationEnabled()) {
            if (!uri.isUseSecureConnection()) {
                throw new RuntimeException("TLS/SSL required for authentication using external authorization");
            }

            // create HTTP client that shares the same settings, but without the external authenticator
            TokenPoller poller = new HttpTokenPoller(builder.build());

            Duration timeout = uri.getExternalAuthenticationTimeout()
                    .map(value -> Duration.ofMillis(value.toMillis()))
                    .orElse(Duration.ofMinutes(2));

            KnownTokenCache knownTokenCache = uri.getExternalAuthenticationTokenCache();
            Optional<RedirectHandler> configuredHandler = uri.getExternalRedirectStrategies()
                    .map(CompositeRedirectHandler::new)
                    .map(RedirectHandler.class::cast);

            RedirectHandler redirectHandler = TrinoUri.getRedirectHandler()
                    .orElseGet(() -> configuredHandler.orElseThrow(() -> new RuntimeException("External authentication redirect handler is not configured")));

            ExternalAuthenticator authenticator = new ExternalAuthenticator(
                    redirectHandler, poller, knownTokenCache.create(), timeout);

            builder.authenticator(authenticator);
            builder.addInterceptor(authenticator);
        }

        uri.getDnsResolver().ifPresent(resolverClass -> builder.dns(instantiateDnsResolver(resolverClass, uri.getDnsResolverContext())::lookup));
        return builder;
    }

    public static OkHttpClient.Builder unauthenticatedClientBuilder(TrinoUri uri, String userAgent)
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        setupUserAgent(builder, userAgent);
        setupSocksProxy(builder, uri.getSocksProxy());
        setupHttpProxy(builder, uri.getHttpProxy());
        setupTimeouts(builder, toIntExact(uri.getTimeout().toMillis()), TimeUnit.MILLISECONDS);
        setupHttpLogging(builder, uri.getHttpLoggingLevel());
        return builder;
    }

    protected static void setupUserAgent(OkHttpClient.Builder builder, String userAgent)
    {
        builder.addInterceptor(userAgent(userAgent));
    }

    private static DnsResolver instantiateDnsResolver(Class<? extends DnsResolver> resolverClass, String context)
    {
        try {
            return resolverClass.getConstructor(String.class).newInstance(context);
        }
        catch (ReflectiveOperationException e) {
            throw new ClientException("Unable to instantiate custom DNS resolver " + resolverClass.getName(), e);
        }
    }
}
