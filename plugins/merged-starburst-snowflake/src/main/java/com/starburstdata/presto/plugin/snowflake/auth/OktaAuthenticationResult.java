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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OktaAuthenticationResult
{
    private final Optional<String> stateToken;
    private final String sessionToken;
    private final String user;

    public OktaAuthenticationResult(String stateToken, String sessionToken, String user)
    {
        this.stateToken = Optional.of(requireNonNull(stateToken, "stateToken is null"));
        this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        this.user = requireNonNull(user, "user is null");
    }

    public OktaAuthenticationResult(String sessionToken, String user)
    {
        this.stateToken = Optional.empty();
        this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        this.user = requireNonNull(user, "user is null");
    }

    public Optional<String> getStateToken()
    {
        return stateToken;
    }

    public String getSessionToken()
    {
        return sessionToken;
    }

    public String getUser()
    {
        return user;
    }
}
