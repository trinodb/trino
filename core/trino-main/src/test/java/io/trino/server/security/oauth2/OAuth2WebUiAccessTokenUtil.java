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

import com.google.common.io.Resources;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.impl.DefaultClaims;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;

import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

public abstract class OAuth2WebUiAccessTokenUtil
{
    public static void validateJwtAccessToken(String cookieValue, String clientId, TestingHydraIdentityProvider hydraIdP)
    {
        assertThat(cookieValue).isNotBlank();
        Jws<Claims> jwt = newJwtParserBuilder()
                .setSigningKeyResolver(new JwkSigningKeyResolver(new JwkService(
                        URI.create("https://localhost:" + hydraIdP.getAuthPort() + "/.well-known/jwks.json"),
                        new JettyHttpClient(new HttpClientConfig()
                                .setTrustStorePath(Resources.getResource("cert/localhost.pem").getPath())))))
                .build()
                .parseClaimsJws(cookieValue);
        Claims claims = jwt.getBody();
        assertThat(claims.getSubject()).isEqualTo("foo@bar.com");
        assertThat(claims.get("client_id")).isEqualTo(clientId);
        assertThat(claims.getIssuer()).isEqualTo("https://localhost:4444/");
    }

    public static void validateOpaqueAccessToken(String cookieValue, String clientId, TestingHydraIdentityProvider hydraIdP)
    {
        Request request = new Request.Builder().url("https://localhost:" + hydraIdP.getAuthPort() + "/userinfo").addHeader(AUTHORIZATION, "Bearer " + cookieValue).build();
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder()
                .followRedirects(false);
        setupInsecureSsl(httpClientBuilder);
        try (Response response = httpClientBuilder.build().newCall(request).execute()) {
            assertThat(response.body()).isNotNull();
            DefaultClaims claims = new DefaultClaims(JsonCodec.mapJsonCodec(String.class, Object.class).fromJson(response.body().bytes()));
            assertThat(claims.getSubject()).isEqualTo("foo@bar.com");
            assertThat(claims.get("aud")).asInstanceOf(list(String.class)).contains(clientId);
        }
        catch (IOException e) {
            fail("Exception while calling /userinfo", e);
        }
    }

    private OAuth2WebUiAccessTokenUtil()
    {
    }
}
