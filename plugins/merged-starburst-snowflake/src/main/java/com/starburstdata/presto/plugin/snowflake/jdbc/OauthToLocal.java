/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocal;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

/**
 * Users authenticate with their Okta user names (usually their email address), which has to match
 * Snowflake's <pre>login_name</pre>.  However, that is typically not the Snowflake user name (which
 * is what's used for example in <pre>DESCRIBE USER</pre>). When an OAuth token is granted, the user
 * name for which it was issued is returned in the response and we use that information to perform
 * the mapping from Okta to Snowflake user name.
 */
public class OauthToLocal
        implements AuthToLocal
{
    private final SnowflakeOauthService snowflakeOauthService;

    public OauthToLocal(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public String translate(ConnectorIdentity identity)
    {
        return snowflakeOauthService.getCredential(identity).getSnowflakeUsername();
    }
}
