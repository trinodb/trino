/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.oracle.OracleConcurrencyType.NO_CONCURRENCY;
import static com.starburstdata.presto.plugin.oracle.OracleConcurrencyType.PARTITIONS;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;

public class OracleSplitManager
{
    private final ConnectionFactory connectionFactory;

    @Inject
    public OracleSplitManager(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    public ConnectorSplitSource getSplitSource(JdbcIdentity identity, JdbcTableHandle tableHandle, OracleConcurrencyType concurrencyType, int maxSplits)
    {
        return new FixedSplitSource(getSplits(identity, tableHandle, concurrencyType, maxSplits));
    }

    public List<OracleSplit> getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle, OracleConcurrencyType concurrencyType, int maxSplits)
    {
        if (concurrencyType == NO_CONCURRENCY) {
            return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty()));
        }

        if (concurrencyType == PARTITIONS) {
            List<String> partitions = listPartitionsForTable(identity, tableHandle);

            if (partitions.isEmpty()) {
                // Table is not partitioned
                return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty()));
            }

            // Partition partitions into batches to limit total number of splits
            return Lists.partition(partitions, IntMath.divide(partitions.size(), maxSplits, CEILING)).stream()
                    .map(batch -> new OracleSplit(Optional.of(batch), Optional.empty()))
                    .collect(toImmutableList());
        }

        throw new IllegalArgumentException(format("Concurrency type %s is not supported", concurrencyType));
    }

    private List<String> listPartitionsForTable(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(identity))) {
            return handle.createQuery("SELECT partition_name FROM all_tab_partitions WHERE table_name = :name AND table_owner = :owner")
                    .bind("name", tableHandle.getTableName())
                    .bind("owner", tableHandle.getSchemaName())
                    .mapTo(String.class)
                    .list();
        }
        catch (JdbiException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
