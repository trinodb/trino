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

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;

import javax.inject.Inject;

public class SapHanaClient
        extends BaseJdbcClient
{
    @Inject
    public SapHanaClient(BaseJdbcConfig baseJdbcConfig, ConnectionFactory connectionFactory)
    {
        super(baseJdbcConfig, "\"", connectionFactory);
    }
}
