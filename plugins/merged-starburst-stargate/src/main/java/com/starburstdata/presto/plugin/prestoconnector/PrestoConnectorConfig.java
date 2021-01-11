/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.airlift.configuration.Config;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD;

// TODO https://starburstdata.atlassian.net/browse/PRESTO-5104 update the name
public class PrestoConnectorConfig
{
    private PrestoAuthenticationType prestoAuthenticationType = PASSWORD;

    public PrestoAuthenticationType getPrestoAuthenticationType()
    {
        return prestoAuthenticationType;
    }

    @Config("starburst.authentication.type")
    public PrestoConnectorConfig setPrestoAuthenticationType(PrestoAuthenticationType prestoAuthenticationType)
    {
        this.prestoAuthenticationType = prestoAuthenticationType;
        return this;
    }
}
