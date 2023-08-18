/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class SalesforceSystemTableProvider
        implements SystemTableProvider
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("system", "limits");
    private static final List<ColumnMetadata> COLUMN_METADATA =
            ImmutableList.of(
                    new ColumnMetadata("type", createUnboundedVarcharType()),
                    new ColumnMetadata("current", BIGINT),
                    new ColumnMetadata("limit", BIGINT));

    private final ConnectionFactory connectionFactory;

    @Inject
    public SalesforceSystemTableProvider(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        return TABLE_NAME;
    }

    @Override
    public SystemTable create()
    {
        return new LimitsSystemTable(connectionFactory);
    }

    private static class LimitsSystemTable
            implements SystemTable
    {
        private final ConnectionFactory connectionFactory;

        public LimitsSystemTable(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        @Override
        public Distribution getDistribution()
        {
            return Distribution.SINGLE_COORDINATOR;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata()
        {
            return new ConnectorTableMetadata(TABLE_NAME, COLUMN_METADATA);
        }

        @Override
        public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
        {
            try (Connection connection = connectionFactory.openConnection(session);
                    CallableStatement statement = connection.prepareCall("GetLimitInfo");
                    ResultSet resultSet = statement.executeQuery()) {
                InMemoryRecordSet.Builder builder = InMemoryRecordSet.builder(COLUMN_METADATA);
                while (resultSet.next()) {
                    builder.addRow(resultSet.getString("type"), resultSet.getLong("current"), resultSet.getLong("limit"));
                }

                return builder.build().cursor();
            }
            catch (SQLException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get Salesforce API limits", e);
            }
        }
    }
}
