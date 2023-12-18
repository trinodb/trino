/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import io.trino.plugin.oracle.BaseOracleConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;

public abstract class BaseUnlicensedStarburstOracleConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
