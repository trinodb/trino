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
package io.trino.client;

import okhttp3.Authenticator;
import okhttp3.Cache;
import okhttp3.Call;
import okhttp3.CertificatePinner;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.CookieJar;
import okhttp3.Dns;
import okhttp3.EventListener;
import okhttp3.Interceptor;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDisallowLocalRedirectInterceptor
{
    private static final String BASE_URL = "https://example.com";

    @Test
    public void testRedirectValidation()
            throws IOException
    {
        DisallowLocalRedirectInterceptor redirector = new DisallowLocalRedirectInterceptor();

        // Valid external URIs
        assertThat(redirector.intercept(chainWithRedirectLocation("https://www.example.com")))
                .isNotNull();

        assertThat(redirector.intercept(chainWithRedirectLocation("https://api.github.com")))
                .isNotNull();

        // Invalid URI
        assertThat(redirector.intercept(chainWithRedirectLocation("not a valid uri")))
                .isNotNull();

        // Unresolvable host
        assertThat(redirector.intercept(chainWithRedirectLocation("https://nonexistent.example.invalid")))
                .isNotNull();

        // Local URIs
        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("https://127.0.0.1")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'https://127.0.0.1' is disallowed");

        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("https://localhost")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'https://localhost' is disallowed");

        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("http://192.168.1.1")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'http://192.168.1.1' is disallowed");

        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("https://0.0.0.0")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'https://0.0.0.0' is disallowed");

        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("https://172.16.0.1/uri")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'https://172.16.0.1/uri' is disallowed");

        assertThatThrownBy(() -> redirector.intercept(chainWithRedirectLocation("https://169.254.169.254")))
                .isInstanceOf(ClientException.class)
                .hasMessage("Following redirect to 'https://169.254.169.254' is disallowed");
    }

    private static Interceptor.Chain chainWithRedirectLocation(String location)
    {
        return new TestingInterceptorChain(new Response.Builder()
                .request(new Request.Builder().url(BASE_URL).build())
                .protocol(Protocol.HTTP_1_1)
                .code(302)
                .message("Found")
                .header("Location", location)
                .build());
    }

    private static class TestingInterceptorChain
            implements Interceptor.Chain
    {
        private final Response response;

        TestingInterceptorChain(Response response)
        {
            this.response = response;
        }

        @Override
        public Request request()
        {
            return response.request();
        }

        @Override
        public Response proceed(Request request)
        {
            return response;
        }

        @Override
        public int connectTimeoutMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withConnectTimeout(int timeout, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readTimeoutMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withReadTimeout(int timeout, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeTimeoutMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withWriteTimeout(int timeout, TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection connection()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Call call()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getFollowSslRedirects()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getFollowRedirects()
        {
            return false;
        }

        @Override
        public Dns getDns()
        {
            return null;
        }

        @Override
        public Interceptor.Chain withDns(Dns dns)
        {
            return this;
        }

        @Override
        public SocketFactory getSocketFactory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withSocketFactory(SocketFactory socketFactory)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getRetryOnConnectionFailure()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withRetryOnConnectionFailure(boolean b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authenticator getAuthenticator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withAuthenticator(Authenticator authenticator)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CookieJar getCookieJar()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withCookieJar(CookieJar cookieJar)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cache getCache()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withCache(Cache cache)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Proxy getProxy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withProxy(Proxy proxy)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProxySelector getProxySelector()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withProxySelector(ProxySelector proxySelector)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authenticator getProxyAuthenticator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withProxyAuthenticator(Authenticator authenticator)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLSocketFactory getSslSocketFactoryOrNull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withSslSocketFactory(SSLSocketFactory sslSocketFactory, X509TrustManager x509TrustManager)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public X509TrustManager getX509TrustManagerOrNull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public HostnameVerifier getHostnameVerifier()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withHostnameVerifier(HostnameVerifier hostnameVerifier)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CertificatePinner getCertificatePinner()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withCertificatePinner(CertificatePinner certificatePinner)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionPool getConnectionPool()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withConnectionPool(ConnectionPool connectionPool)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventListener getEventListener()
        {
            throw new UnsupportedOperationException();
        }
    }
}
