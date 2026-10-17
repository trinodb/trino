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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TestOAuth2WebUiAuthenticationFilterWithRequiredAudience
        extends TestOAuth2WebUiAuthenticationFilterWithJwt
{
    @Override
    protected Map<String, String> getOAuth2Config(String idpUrl)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(super.getOAuth2Config(idpUrl))
                .put("http-server.authentication.oauth2.require-audience", "true")
                .buildOrThrow();
    }

    @Override
    protected TestingHydraIdentityProvider getHydraIdp()
            throws Exception
    {
        TestingHydraIdentityProvider hydraIdP = new TestingHydraIdentityProvider(TTL_ACCESS_TOKEN_IN_SECONDS, true, false, true);
        hydraIdP.start();

        return hydraIdP;
    }

    @Override
    protected boolean isAudienceRequired()
    {
        return true;
    }
}
