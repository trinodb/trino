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
package io.trino.server.ui;

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static io.trino.server.ui.MultipartUiCookie.MAXIMUM_COOKIE_SIZE;
import static jakarta.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;
import static org.assertj.core.api.Assertions.assertThat;

class TestMultipartUiCookie
{
    private static final String COOKIE_NAME = "__UI_Cookie";

    private static final MultipartUiCookie COOKIE = new MultipartUiCookie(COOKIE_NAME, "/location");

    @Test
    public void testCookieNames()
    {
        assertThat(COOKIE.cookieName(0)).isEqualTo(COOKIE_NAME);
        assertThat(COOKIE.cookieName(1)).isEqualTo(COOKIE_NAME + "_1");
        assertThat(COOKIE.cookieName(500)).isEqualTo(COOKIE_NAME + "_500");
    }

    @Test
    public void testShortValueRoundTrip()
    {
        String longToken = "123456789".repeat(100); // < 4096 cookie value length limit
        NewCookie[] newCookies = COOKIE.create(longToken, Instant.EPOCH, true);
        assertThat(newCookies).hasSize(1);
        assertThat(newCookies).extracting(NewCookie::getName).hasSameElementsAs(List.of(COOKIE.cookieName(0)));
        assertThat(COOKIE.read(cookies(newCookies))).contains(longToken);

        NewCookie[] deleteCookies = COOKIE.delete(cookies(newCookies), true);
        assertThat(deleteCookies).hasSize(1);
        assertThat(deleteCookies).extracting(NewCookie::getName).hasSameElementsAs(List.of(COOKIE.cookieName(0)));
    }

    @Test
    public void testLongValueRoundTrip()
    {
        String longToken = "123456789".repeat(1000); // > 4096 cookie value length limit
        NewCookie[] newCookies = COOKIE.create(longToken, Instant.EPOCH, true);
        assertThat(newCookies)
                .hasSize(3)
                .allMatch(NewCookie::isSecure)
                .allMatch(NewCookie::isHttpOnly)
                .allMatch(cookie -> cookie.getMaxAge() == DEFAULT_MAX_AGE)
                .allMatch(cookie -> cookie.getExpiry().equals(Date.from(Instant.EPOCH)))
                .allMatch(cookie -> (cookie.getName().length() + cookie.getValue().length()) <= MAXIMUM_COOKIE_SIZE);

        assertThat(newCookies)
                .extracting(NewCookie::getName)
                .hasSameElementsAs(List.of(COOKIE.cookieName(0), COOKIE.cookieName(1), COOKIE.cookieName(2)));

        assertThat(COOKIE.read(cookies(newCookies))).contains(longToken);

        NewCookie[] deleteCookies = COOKIE.delete(cookies(newCookies), false);
        assertThat(deleteCookies)
                .hasSize(3)
                .allMatch(cookie -> !cookie.isSecure())
                .extracting(NewCookie::getName)
                .hasSameElementsAs(List.of(COOKIE.cookieName(0), COOKIE.cookieName(1), COOKIE.cookieName(2)));
    }

    @Test
    public void testMultipartValueRead()
    {
        assertThat(COOKIE.read(cookies(cookie(0, "a"), cookie(1, "b")))).contains("ab");
        assertThat(COOKIE.read(cookies(cookie(0, "a"), cookie(1, "b"), cookie(2, "c")))).contains("abc");
        assertThat(COOKIE.read(cookies(cookie(1, "b"), cookie(0, "a"), cookie(2, "c")))).contains("abc");
        assertThat(COOKIE.read(cookies(cookie(2, "c"), cookie(1, "b"), cookie(0, "a"), collidingCookie(0, "d")))).contains("abc");
    }

    @Test
    public void testNonContinuousValueRead()
    {
        assertThat(COOKIE.read(cookies(cookie(0, "a"), cookie(2, "b")))).isEmpty();
        assertThat(COOKIE.read(cookies(cookie(0, "a"), cookie(1, "b"), cookie(3, "a")))).isEmpty();
        assertThat(COOKIE.read(cookies(cookie(1, "a")))).isEmpty();
    }

    @Test
    public void testDelete()
    {
        assertThat(COOKIE.delete(cookies(cookie(0, "a"), cookie(2, "b"), collidingCookie(3, "d")), true))
                .hasSize(2)
                .allMatch(cookie -> cookie.getMaxAge() == 0)
                .allMatch(cookie -> cookie.getValue().equals("delete"))
                .allMatch(NewCookie::isSecure)
                .allMatch(NewCookie::isHttpOnly);
        assertThat(COOKIE.delete(cookies(cookie(0, "a"), cookie(2, "b"), cookie("some-other-cookie", "some-other-value")), false))
                .hasSize(2)
                .allMatch(cookie -> cookie.getName().startsWith(COOKIE_NAME))
                .allMatch(cookie -> cookie.getMaxAge() == 0)
                .allMatch(cookie -> cookie.getValue().equals("delete"))
                .allMatch(cookie -> !cookie.isSecure())
                .allMatch(NewCookie::isHttpOnly);
        assertThat(COOKIE.delete(cookies(), false))
                .hasSize(1)
                .allMatch(cookie -> cookie.getName().startsWith(COOKIE_NAME))
                .allMatch(cookie -> cookie.getMaxAge() == 0)
                .allMatch(cookie -> cookie.getValue().equals("delete"))
                .allMatch(cookie -> !cookie.isSecure())
                .allMatch(NewCookie::isHttpOnly);
    }

    private static Map<String, Cookie> cookies(NewCookie... cookies)
    {
        ImmutableMap.Builder<String, Cookie> result = ImmutableMap.builderWithExpectedSize(cookies.length);
        for (NewCookie cookie : cookies) {
            result.put(cookie.getName(), cookie);
        }
        return result.buildOrThrow();
    }

    private static NewCookie cookie(int index, String value)
    {
        return cookie(COOKIE.cookieName(index), value);
    }

    private static NewCookie collidingCookie(int index, String value)
    {
        return cookie(COOKIE.cookieName(index) + "-ignored", value);
    }

    private static NewCookie cookie(String name, String value)
    {
        return new NewCookie.Builder(name)
                .value(value)
                .build();
    }
}
