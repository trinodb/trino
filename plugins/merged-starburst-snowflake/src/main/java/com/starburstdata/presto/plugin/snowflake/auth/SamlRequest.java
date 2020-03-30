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
package com.starburstdata.presto.plugin.snowflake.auth;

import static java.util.Objects.requireNonNull;

public class SamlRequest
{
    private final String redirectUrl;
    private final String oauthSessionStorageData;

    public SamlRequest(String redirectUrl, String oauthSessionStorageData)
    {
        this.redirectUrl = requireNonNull(redirectUrl, "redirectUrl is null");
        this.oauthSessionStorageData = requireNonNull(oauthSessionStorageData, "oauthSessionStorageData is null");
    }

    public String getRedirectUrl()
    {
        return redirectUrl;
    }

    public String getOauthSessionStorageData()
    {
        return oauthSessionStorageData;
    }
}
