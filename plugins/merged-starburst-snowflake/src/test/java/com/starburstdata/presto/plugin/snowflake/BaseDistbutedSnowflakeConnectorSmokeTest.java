/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.testing.BaseConnectorSmokeTest;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDistbutedSnowflakeConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE region").getOnlyValue())
                .isEqualTo("CREATE TABLE snowflake.test_schema_2.region (\n" +
                        "   regionkey decimal(19, 0),\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")");
    }
}
