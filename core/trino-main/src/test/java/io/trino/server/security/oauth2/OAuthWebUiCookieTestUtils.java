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
package io.trino.server.security.oauth2;

import java.net.CookieStore;
import java.net.HttpCookie;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public final class OAuthWebUiCookieTestUtils
{
    public static void assertWebUiCookie(CookieStore cookieStore, String cookieName, Optional<String> expectedCookieValue, Optional<Long> cookieTtlSeconds)
    {
        HttpCookie cookie = getWebUiCookieRequired(cookieStore, cookieName);
        assertWebUiCookie(cookie, expectedCookieValue, cookieTtlSeconds);
    }

    public static void assertWebUiCookie(HttpCookie cookie, Optional<String> expectedCookieValue, Optional<Long> cookieTtlSeconds)
    {
        if (expectedCookieValue.isPresent()) {
            assertEquals(cookie.getValue(), expectedCookieValue.get());
        }
        assertThat(cookie.getDomain()).isIn("127.0.0.1", "::1");
        assertThat(cookie.getPath()).isEqualTo("/ui/");
        assertThat(cookie.getSecure()).isTrue();
        assertThat(cookie.isHttpOnly()).isTrue();
        if (cookieTtlSeconds.isPresent()) {
            assertThat(cookie.getMaxAge()).isLessThanOrEqualTo(cookieTtlSeconds.get());
            assertThat(cookie.getMaxAge()).isGreaterThan(0);
        }
    }

    public static HttpCookie getWebUiCookieRequired(CookieStore cookieStore, String cookieName)
    {
        return getWbUiCookie(cookieStore, cookieName)
                .orElseThrow(() -> new IllegalArgumentException("No cookie with name: " + cookieName));
    }

    public static Optional<HttpCookie> getWbUiCookie(CookieStore cookieStore, String cookieName)
    {
        return cookieStore.getCookies()
                .stream()
                .filter(cookie -> cookie.getName().equals(cookieName))
                .findFirst();
    }

    private OAuthWebUiCookieTestUtils()
    {
    }
}
