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

public class SamlResponse
{
    private final String samlAssertion;
    private final String oauthSessionStorageData;
    private final String user;

    public SamlResponse(String samlAssertion, String oauthSessionStorageData, String user)
    {
        this.samlAssertion = requireNonNull(samlAssertion, "samlAssertion is null");
        this.oauthSessionStorageData = requireNonNull(oauthSessionStorageData, "oauthSessionStorageData is null");
        this.user = requireNonNull(user, "user is null");
    }

    public String getSamlAssertion()
    {
        return samlAssertion;
    }

    public String getOauthSessionStorageData()
    {
        return oauthSessionStorageData;
    }

    public String getUser()
    {
        return user;
    }
}
