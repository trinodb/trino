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

import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.CookieManager;
import java.net.CookieStore;
import java.net.HttpCookie;
import java.util.Optional;

import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.oauth2.OAuthWebUiCookieTestUtils.assertWebUiCookie;
import static io.trino.server.security.oauth2.OAuthWebUiCookieTestUtils.getWebUiCookieRequired;
import static io.trino.server.ui.OAuthRefreshWebUiCookie.OAUTH2_REFRESH_COOKIE;
import static io.trino.server.ui.OAuthWebUiCookie.OAUTH2_COOKIE;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class BaseOAuth2WebUiAuthenticationFilterWithTokenRefreshTest
        extends BaseOAuth2WebUiAuthenticationFilterTest
{
    @Override
    protected void assertWebUiCookies(CookieStore cookieStore)
            throws IOException
    {
        super.assertWebUiCookies(cookieStore);
        assertWebUiCookie(cookieStore, OAUTH2_REFRESH_COOKIE, Optional.empty(), Optional.empty());
    }

    @Test
    @Override
    public void testExpiredAccessToken()
            throws Exception
    {
        CookieManager cookieManager = new CookieManager();
        CookieStore cookieStore = cookieManager.getCookieStore();

        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        OkHttpClient httpClient = httpClientBuilder
                .followRedirects(true)
                .cookieJar(new JavaNetCookieJar(cookieManager))
                .build();

        assertThat(cookieStore.get(uiUri)).isEmpty();

        // access UI and follow redirects in order to get OAuth2 cookie
        Request uiRequest = new Request.Builder()
                .url(uiUri.toURL())
                .get()
                .build();

        Response response = httpClient.newCall(uiRequest).execute();
        assertEquals(response.code(), SC_OK);
        assertEquals(response.request().url().toString(), uiUri.toString());
        assertWebUiCookies(cookieStore);

        HttpCookie oauth2Cookie = getWebUiCookieRequired(cookieStore, OAUTH2_COOKIE);
        HttpCookie oauth2RefreshCookie = getWebUiCookieRequired(cookieStore, OAUTH2_REFRESH_COOKIE);

        // wait for the token expiration
        Thread.sleep(TTL_ACCESS_TOKEN_IN_SECONDS.plusSeconds(1).toMillis());

        response = httpClient.newCall(uiRequest).execute();
        assertEquals(response.code(), SC_OK);
        assertEquals(response.request().url().toString(), uiUri.toString());
        assertWebUiCookies(cookieStore);

        // Verify that we actually refreshed tokens to new once
        assertThat(getWebUiCookieRequired(cookieStore, OAUTH2_COOKIE).getValue()).isNotEqualTo(oauth2Cookie.getValue());
        assertThat(getWebUiCookieRequired(cookieStore, OAUTH2_REFRESH_COOKIE).getValue()).isNotEqualTo(oauth2RefreshCookie.getValue());
    }
}
