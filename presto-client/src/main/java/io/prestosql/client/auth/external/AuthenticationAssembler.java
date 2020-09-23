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
package io.prestosql.client.auth.external;

import java.net.URI;

import static java.util.Objects.requireNonNull;

class AuthenticationAssembler
{
    private static final String REDIRECT_URL_FIELD = "redirectUrl=";
    private static final String TOKEN_PATH_FIELD = "tokenUrl=";

    private final String baseUri;

    AuthenticationAssembler(URI baseUri)
    {
        this.baseUri = requireNonNull(baseUri.toString(), "baseUri is null");
    }

    ExternalAuthentication toAuthentication(String header)
    {
        int space = header.indexOf(' ');
        String ssoData = header.substring(space + 1);
        String[] fields = ssoData.split(", ");
        String tokenPath = null;
        String redirectUrl = null;
        for (String field : fields) {
            if (field.startsWith(REDIRECT_URL_FIELD)) {
                redirectUrl = relativeOrAbsolute(extractFieldValue(field, REDIRECT_URL_FIELD));
            }
            else if (field.startsWith(TOKEN_PATH_FIELD)) {
                tokenPath = relativeOrAbsolute(extractFieldValue(field, TOKEN_PATH_FIELD));
            }
        }
        return new ExternalAuthentication(redirectUrl, tokenPath);
    }

    private String relativeOrAbsolute(String url)
    {
        if (url.startsWith("/")) {
            return baseUri + url;
        }
        return url;
    }

    private static String extractFieldValue(String field, String fieldKey)
    {
        return field.substring(fieldKey.length() + 1, field.length() - 1);
    }
}
