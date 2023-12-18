/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse.faulttolerant;

import io.trino.operator.RetryPolicy;

public class TestSynapseQueryFailureRecoveryTest
        extends BaseSynapseFailureRecoveryTest
{
    public TestSynapseQueryFailureRecoveryTest()
    {
        super(RetryPolicy.QUERY);
    }
}
