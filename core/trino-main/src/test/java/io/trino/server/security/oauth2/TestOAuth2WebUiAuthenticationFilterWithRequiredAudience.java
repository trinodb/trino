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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestOAuth2WebUiAuthenticationFilterWithRequiredAudience
        extends BaseOAuth2WebUiAuthenticationFilterWithJwtTest
{
    TestOAuth2WebUiAuthenticationFilterWithRequiredAudience()
    {
        super(true);
    }

    @Override
    protected Map<String, String> getOAuth2Config(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(super.getOAuth2Config(idpUrl))
                .put("http-server.authentication.oauth2.require-audience", "true")
                .buildOrThrow();
    }

    @Test
    void testTokenWithoutAudience()
            throws Exception
    {
        String token = hydraIdP.getToken(NO_AUDIENCE_CLIENT_ID, NO_AUDIENCE_CLIENT_SECRET, ImmutableList.of());
        assertThat(parseJwsClaims(token).getPayload().getAudience()).isNull();
        try (Response response = httpClientWithOAuth2Cookie(token, false)
                .newCall(uiCall().build())
                .execute()) {
            assertRedirectResponse(response);
        }
    }
}
