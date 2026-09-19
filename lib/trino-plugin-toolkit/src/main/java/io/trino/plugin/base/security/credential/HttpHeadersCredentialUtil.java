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
package io.trino.plugin.base.security.credential;

import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.trino.spi.security.credential.HttpHeadersCredential;

public class HttpHeadersCredentialUtil
{
    private HttpHeadersCredentialUtil() {}

    public static Request.Builder applyHeaders(Request.Builder request, HttpHeadersCredential header)
    {
        header.getHeaders().forEach((key, value) -> request.setHeader(HeaderName.of(key), value));
        return request;
    }
}
