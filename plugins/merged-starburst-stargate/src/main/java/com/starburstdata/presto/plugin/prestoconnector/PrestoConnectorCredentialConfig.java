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

import io.trino.plugin.jdbc.credential.CredentialConfig;

import javax.validation.constraints.AssertTrue;

public class PrestoConnectorCredentialConfig
        extends CredentialConfig
{
    @AssertTrue(message = "Connection user is not configured")
    public boolean isUserConfigured()
    {
        return getConnectionUser().isPresent();
    }
}
