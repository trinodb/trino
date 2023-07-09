/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class SapHanaAuthenticationConfig
{
    private SapHanaAuthenticationType authenticationType = SapHanaAuthenticationType.PASSWORD;

    @NotNull
    public SapHanaAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("sap-hana.authentication.type")
    @ConfigDescription("SAP HANA authentication mechanism type")
    public SapHanaAuthenticationConfig setAuthenticationType(SapHanaAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }
}
