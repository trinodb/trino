/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
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
