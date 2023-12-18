/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.core.ParameterBindingDTO;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class StarburstSnowflakeStatementV1
        extends SnowflakePreparedStatementV1
{
    public StarburstSnowflakeStatementV1(SnowflakeConnectionV1 connection, String sql)
            throws SQLException
    {
        super(connection, sql, true, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * Expose Snowflake parameter bindings
     */
    @Override
    public Map<String, ParameterBindingDTO> getParameterBindings()
    {
        return super.getParameterBindings();
    }
}
