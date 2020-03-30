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

public class OauthCredential
{
    private final String accessToken;
    private final long accessTokenExpirationTime;
    private final String refreshToken;
    private final long refreshTokenExpirationTime;
    private final String snowflakeUsername;

    public OauthCredential(String accessToken, long accessTokenExpirationTime, String refreshToken, long refreshTokenExpirationTime, String snowflakeUsername)
    {
        this.accessToken = requireNonNull(accessToken, "accessToken is null");
        this.accessTokenExpirationTime = accessTokenExpirationTime;
        this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
        this.refreshTokenExpirationTime = refreshTokenExpirationTime;
        this.snowflakeUsername = requireNonNull(snowflakeUsername, "snowflakeUsername is null");
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    public long getAccessTokenExpirationTime()
    {
        return accessTokenExpirationTime;
    }

    public String getRefreshToken()
    {
        return refreshToken;
    }

    public long getRefreshTokenExpirationTime()
    {
        return refreshTokenExpirationTime;
    }

    public String getSnowflakeUsername()
    {
        return snowflakeUsername;
    }
}
