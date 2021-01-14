/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.PrestoException;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class StarburstSynapseClient
        extends StarburstSqlServerClient
{
    private static final int MAX_NVARCHAR_LENGTH = 4000;
    private static final int MAX_NCHAR_LENGTH = 4000;
    private static final int MAX_VARBINARY_LENGTH = 8000;

    @Inject
    public StarburstSynapseClient(BaseJdbcConfig config, JdbcStatisticsConfig statisticsConfig, TableScanRedirection tableScanRedirection, ConnectionFactory connectionFactory)
    {
        super(config, statisticsConfig, tableScanRedirection, connectionFactory);
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported");
        }
        String sql = format(
                "RENAME OBJECT %s TO %s",
                quoted(catalogName, schemaName, tableName),
                newTable.getTableName());
        execute(session, sql);
    }

    // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5074) Add support for DATETIME2 mapping to Synapse Connector

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "nvarchar(" + MAX_NVARCHAR_LENGTH + ")";
            }
            else if (varcharType.getBoundedLength() > MAX_NVARCHAR_LENGTH) {
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > MAX_NCHAR_LENGTH) {
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(" + MAX_VARBINARY_LENGTH + ")", varbinaryWriteFunction());
        }

        return super.toWriteMapping(session, type);
    }
}
