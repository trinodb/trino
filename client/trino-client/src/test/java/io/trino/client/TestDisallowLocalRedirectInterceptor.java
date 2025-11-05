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

import okhttp3.Interceptor;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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
        public Interceptor.Chain withConnectTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readTimeoutMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withReadTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeTimeoutMillis()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Interceptor.Chain withWriteTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public okhttp3.Connection connection()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public okhttp3.Call call()
        {
            throw new UnsupportedOperationException();
        }
    }
}
