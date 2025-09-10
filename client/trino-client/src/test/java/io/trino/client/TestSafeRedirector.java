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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSafeRedirector
{
    private static final String BASE_URL = "https://example.com";

    @Test
    public void testRedirectValidation()
    {
        SafeRedirector redirector = new SafeRedirector();

        // Test valid external redirects
        assertThatCode(() ->
            redirector.intercept(createChainWithRedirect("https://www.example.com")))
                .doesNotThrowAnyException();
        assertThatCode(() ->
            redirector.intercept(createChainWithRedirect("https://api.github.com")))
                .doesNotThrowAnyException();

        // Test invalid URI
        assertThatCode(() ->
            redirector.intercept(createChainWithRedirect("not a valid uri")))
                .doesNotThrowAnyException();
        // Test unresolvable host
        assertThatCode(() ->
            redirector.intercept(createChainWithRedirect("https://nonexistent.example.invalid")))
                .doesNotThrowAnyException();

        // Test denied uri's
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("https://127.0.0.1")))
                .isInstanceOf(ClientException.class);
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("https://localhost")))
                .isInstanceOf(ClientException.class);
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("http://192.168.1.1")))
                .isInstanceOf(ClientException.class);
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("https://0.0.0.0")))
                .isInstanceOf(ClientException.class);
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("https://172.16.0.1")))
                .isInstanceOf(ClientException.class);
        assertThatThrownBy(() ->
            redirector.intercept(createChainWithRedirect("https://169.254.169.254")))
                .isInstanceOf(ClientException.class);
    }

    private static Interceptor.Chain createChainWithRedirect(String location)
    {
        return new TestInterceptorChain(new Response.Builder()
                .request(new Request.Builder().url(BASE_URL).build())
                .protocol(Protocol.HTTP_1_1)
                .code(302)
                .message("Found")
                .header("Location", location)
                .build());
    }

    private static class TestInterceptorChain
            implements Interceptor.Chain
    {
        private final Response response;

        TestInterceptorChain(Response response)
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

        // Implement other Chain methods with default values
        @Override
        public int connectTimeoutMillis()
        {
            return 0;
        }

        @Override
        public Interceptor.Chain withConnectTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            return this;
        }

        @Override
        public int readTimeoutMillis()
        {
            return 0;
        }

        @Override
        public Interceptor.Chain withReadTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            return this;
        }

        @Override
        public int writeTimeoutMillis()
        {
            return 0;
        }

        @Override
        public Interceptor.Chain withWriteTimeout(int timeout, java.util.concurrent.TimeUnit unit)
        {
            return this;
        }

        @Override
        public okhttp3.Connection connection()
        {
            return null;
        }

        @Override
        public okhttp3.Call call()
        {
            return null;
        }
    }
}
