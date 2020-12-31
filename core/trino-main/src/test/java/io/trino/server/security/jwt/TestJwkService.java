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
package io.prestosql.server.security.jwt;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJwkService
{
    private static final String EMPTY_KEYS = "{ \"keys\": [] }";
    private static final String TEST_JWK_RESPONSE = "" +
            "{\n" +
            "  \"keys\": [\n" +
            "    {\n" +
            "      \"e\": \"AQAB\",\n" +
            "      \"n\": \"mvj-0waJ2owQlFWrlC06goLs9PcNehIzCF0QrkdsYZJXOsipcHCFlXBsgQIdTdLvlCzNI07jSYA-zggycYi96lfDX-FYv_CqC8dRLf9TBOPvUgCyFMCFNUTC69hsrEYMR_J79Wj0MIOffiVr6eX-AaCG3KhBMZMh15KCdn3uVrl9coQivy7bk2Uw-aUJ_b26C0gWYj1DnpO4UEEKBk1X-lpeUMh0B_XorqWeq0NYK2pN6CoEIh0UrzYKlGfdnMU1pJJCsNxMiha-Vw3qqxez6oytOV_AswlWvQc7TkSX6cHfqepNskQb7pGxpgQpy9sA34oIxB_S-O7VS7_h0Qh4vQ\",\n" +
            "      \"alg\": \"RS256\",\n" +
            "      \"use\": \"sig\",\n" +
            "      \"kty\": \"RSA\",\n" +
            "      \"kid\": \"test-rsa\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"kty\": \"EC\",\n" +
            "      \"use\": \"sig\",\n" +
            "      \"crv\": \"P-256\",\n" +
            "      \"kid\": \"test-ec\",\n" +
            "      \"x\": \"W9pnAHwUz81LldKjL3BzxO1iHe1Pc0fO6rHkrybVy6Y\",\n" +
            "      \"y\": \"XKSNmn_xajgOvWuAiJnWx5I46IwPVJJYPaEpsX3NPZg\",\n" +
            "      \"alg\": \"ES256\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Test
    public void testSuccess()
    {
        HttpClient httpClient = new TestingHttpClient(request -> mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
        JwkService service = new JwkService(URI.create("http://example.com"), httpClient, new Duration(1, DAYS));
        assertTestKeys(service);
    }

    @Test
    public void testReload()
    {
        AtomicReference<Response> response = new AtomicReference<>(mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        JwkService service = new JwkService(URI.create("http://example.com"), new TestingHttpClient(request -> response.get()), new Duration(1, DAYS));
        assertEmptyKeys(service);

        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        service.refreshKeys();
        assertEmptyKeys(service);

        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
        service.refreshKeys();
        assertTestKeys(service);

        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
        service.refreshKeys();
        assertTestKeys(service);

        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        service.refreshKeys();
        assertEmptyKeys(service);
    }

    @Test
    public void testTimedReload()
            throws InterruptedException
    {
        AtomicReference<Supplier<Response>> response = new AtomicReference<>(() -> mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        JwkService service = new JwkService(URI.create("http://example.com"), new TestingHttpClient(request -> response.get().get()), new Duration(1, MILLISECONDS));
        assertEmptyKeys(service);

        try {
            // start service
            response.set(() -> mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
            service.start();
            while (!service.getKeys().isEmpty()) {
                //noinspection BusyWait
                Thread.sleep(1000);
            }

            response.set(() -> mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
            while (service.getKeys().isEmpty()) {
                //noinspection BusyWait
                Thread.sleep(1000);
            }
            assertTestKeys(service);
        }
        finally {
            service.stop();
        }
    }

    @Test
    public void testRequestFailure()
    {
        AtomicReference<Response> response = new AtomicReference<>(mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
        JwkService service = new JwkService(
                URI.create("http://example.com"),
                new TestingHttpClient(request -> {
                    Response value = response.get();
                    if (value == null) {
                        throw new IllegalArgumentException("test");
                    }
                    return value;
                }),
                new Duration(1, DAYS));
        assertTestKeys(service);

        // request failure
        response.set(null);
        assertThatThrownBy(service::refreshKeys)
                .hasMessage("Error reading JWK keys from http://example.com")
                .isInstanceOf(RuntimeException.class);
        assertTestKeys(service);

        // valid update
        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        service.refreshKeys();
        assertEmptyKeys(service);
    }

    @Test
    public void testBadResponse()
    {
        AtomicReference<Response> response = new AtomicReference<>(mockResponse(HttpStatus.OK, JSON_UTF_8, TEST_JWK_RESPONSE));
        JwkService service = new JwkService(URI.create("http://example.com"), new TestingHttpClient(request -> response.get()), new Duration(1, DAYS));
        assertTestKeys(service);

        // bad response code document
        response.set(mockResponse(HttpStatus.CREATED, JSON_UTF_8, ""));
        assertThatThrownBy(service::refreshKeys)
                .hasMessage("Unexpected response code 201 from JWK service at http://example.com")
                .isInstanceOf(RuntimeException.class);
        assertTestKeys(service);

        // empty document
        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, ""));
        assertThatThrownBy(service::refreshKeys)
                .hasMessage("Unable to decode JWK response from http://example.com")
                .isInstanceOf(RuntimeException.class);
        assertTestKeys(service);

        // valid update
        response.set(mockResponse(HttpStatus.OK, JSON_UTF_8, EMPTY_KEYS));
        service.refreshKeys();
        assertEmptyKeys(service);
    }

    private static void assertEmptyKeys(JwkService service)
    {
        assertEquals(service.getKeys().size(), 0);
    }

    private static void assertTestKeys(JwkService service)
    {
        Map<String, PublicKey> keys = service.getKeys();
        assertEquals(keys.size(), 2);
        assertTrue(keys.containsKey("test-rsa"));
        assertTrue(keys.containsKey("test-ec"));
    }
}
