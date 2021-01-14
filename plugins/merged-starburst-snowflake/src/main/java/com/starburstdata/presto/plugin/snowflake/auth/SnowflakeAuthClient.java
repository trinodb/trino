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

import io.trino.plugin.jdbc.JdbcIdentity;

public interface SnowflakeAuthClient
{
    SamlRequest generateSamlRequest(JdbcIdentity identity);

    OauthCredential requestOauthToken(SamlResponse samlResponse);

    OauthCredential refreshCredential(OauthCredential credential);
}
