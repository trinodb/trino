/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import io.trino.plugin.jdbc.credential.CredentialConfig;
import jakarta.validation.constraints.AssertTrue;

public class StargateCredentialConfig
        extends CredentialConfig
{
    @AssertTrue(message = "Connection user is not configured")
    public boolean isUserConfigured()
    {
        return getConnectionUser().isPresent();
    }
}
