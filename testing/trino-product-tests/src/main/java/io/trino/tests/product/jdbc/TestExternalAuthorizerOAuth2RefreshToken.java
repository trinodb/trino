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
package io.trino.tests.product.jdbc;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.jdbc.TestingRedirectHandlerInjector;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tests.product.TpchTableResults;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.CookieManager;
import java.net.URI;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.tempto.query.QueryResult.forResultSet;
import static io.trino.tests.product.TestGroups.OAUTH2_REFRESH;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExternalAuthorizerOAuth2RefreshToken
        extends ProductTest
{
    @Inject
    @Named("databases.presto.jdbc_url")
    String jdbcUrl;

    @Inject
    @Named("databases.presto.https_keystore_path")
    String truststorePath;

    @Inject
    @Named("databases.presto.https_keystore_password")
    String truststorePassword;

    private OkHttpClient httpClient;

    @BeforeMethodWithContext
    public void setUp()
            throws Exception
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        KeyStore keyStore = KeyStore.getInstance(new File(truststorePath), truststorePassword.toCharArray());
        HandshakeCertificates.Builder certificatesBuilder = new HandshakeCertificates.Builder();
        keyStore.aliases().asIterator().forEachRemaining(alias -> {
            try {
                Certificate certificate = keyStore.getCertificate(alias);
                if (certificate instanceof X509Certificate) {
                    certificatesBuilder.addTrustedCertificate((X509Certificate) certificate);
                }
            }
            catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
        });
        HandshakeCertificates certificates = certificatesBuilder.build();
        httpClientBuilder.sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager());
        httpClientBuilder.followRedirects(true);
        httpClientBuilder.cookieJar(new JavaNetCookieJar(new CookieManager()));
        httpClient = httpClientBuilder.build();
    }

    @Test(groups = {OAUTH2_REFRESH, PROFILE_SPECIFIC_TESTS})
    public void shouldRefreshTokenAfterTokenExpire()
            throws Exception
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler();
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertThat(forResultSet(results)).matches(TpchTableResults.PRESTO_NATION_RESULT);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            //Wait until the token expires. See: HydraIdentityProvider.TTL_ACCESS_TOKEN_IN_SECONDS
            SECONDS.sleep(10);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                assertThat(forResultSet(repeatedResults)).matches(TpchTableResults.PRESTO_NATION_RESULT);
            }

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);
        }
    }

    @Test(groups = {OAUTH2_REFRESH, PROFILE_SPECIFIC_TESTS})
    public void shouldAuthenticateAfterRefreshTokenExpires()
            throws Exception
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler();
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertThat(forResultSet(results)).matches(TpchTableResults.PRESTO_NATION_RESULT);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            //Wait until the refresh token expires (15s) . See: HydraIdentityProvider.TTL_REFRESH_TOKEN_IN_SECONDS
            SECONDS.sleep(20);
            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                assertThat(forResultSet(repeatedResults)).matches(TpchTableResults.PRESTO_NATION_RESULT);
            }

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(2);
        }
    }

    @Test(groups = {OAUTH2_REFRESH, PROFILE_SPECIFIC_TESTS})
    public void shouldAuthenticateAfterIssuedTokenExpires()
            throws Exception
    {
        CountingRedirectHandler redirectHandler = new CountingRedirectHandler();
        TestingRedirectHandlerInjector.setRedirectHandler(redirectHandler);

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertThat(forResultSet(results)).matches(TpchTableResults.PRESTO_NATION_RESULT);

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(1);

            //Wait until the internally issued token expires. See: http-server.authentication.oauth2.refresh-tokens.issued-token.timeout
            SECONDS.sleep(35);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                assertThat(forResultSet(repeatedResults)).matches(TpchTableResults.PRESTO_NATION_RESULT);
            }

            assertThat(redirectHandler.getRedirectCount()).isEqualTo(2);
        }
    }

    private class CountingRedirectHandler
            implements Consumer<URI>
    {
        private final AtomicInteger redirectCount = new AtomicInteger();

        @Override
        public void accept(URI uri)
        {
            redirectCount.incrementAndGet();
            try (Response response = httpClient.newCall(
                    new Request.Builder()
                            .get()
                            .url(uri.toString())
                            .build())
                    .execute()) {
                int statusCode = response.code();
                checkState(statusCode == 200, "Invalid status %s", statusCode);
                requireNonNull(response.body(), "body is null");
                String body = response.body().string();
                checkState(body.contains("OAuth2 authentication succeeded"), "Invalid response %s", body);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public int getRedirectCount()
        {
            return redirectCount.get();
        }
    }
}
