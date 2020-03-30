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
