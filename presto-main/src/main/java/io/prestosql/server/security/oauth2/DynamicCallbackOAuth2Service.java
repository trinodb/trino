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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthConstants;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.oauth.AccessTokenRequestParams;
import com.github.scribejava.core.oauth.OAuth20Service;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DynamicCallbackOAuth2Service
        extends OAuth20Service
{
    public DynamicCallbackOAuth2Service(
            DefaultApi20 api,
            String apiKey,
            String apiSecret,
            String defaultScope)
    {
        super(api, apiKey, apiSecret, null, defaultScope, "code", null, null, null, null);
    }

    public OAuth2AccessToken getAccessToken(String code, String callbackUrl)
            throws IOException, InterruptedException, ExecutionException
    {
        OAuthRequest request = createAccessTokenRequest(AccessTokenRequestParams.create(code));
        request.addParameter(OAuthConstants.REDIRECT_URI, callbackUrl);
        return sendAccessTokenRequestSync(request);
    }
}
