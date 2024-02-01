/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.starburstdata.trino.plugin.license.LicenseManager;

public class TestingSapHanaPlugin
        extends SapHanaPlugin
{
    public static final LicenseManager NOOP_LICENSE_MANAGER = () -> true;

    public TestingSapHanaPlugin()
    {
        super(NOOP_LICENSE_MANAGER);
    }
}
