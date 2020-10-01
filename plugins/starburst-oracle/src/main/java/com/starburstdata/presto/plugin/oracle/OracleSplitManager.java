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
import com.google.common.collect.Multiset;
import com.google.common.math.IntMath;
import com.starburstdata.presto.license.LicenseManager;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.TupleDomain;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.starburstdata.presto.license.StarburstPrestoFeature.ORACLE_EXTENSIONS;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.NO_PARALLELISM;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.PARTITIONS;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.getMaxSplitsPerScan;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.getParallelismType;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;

public class OracleSplitManager
        implements ConnectorSplitManager
{
    private final ConnectionFactory connectionFactory;

    @Inject
    public OracleSplitManager(
            ConnectionFactory connectionFactory,
            StarburstOracleConfig starburstOracleConfig,
            LicenseManager licenseManager)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        if (starburstOracleConfig.getParallelismType() != OracleParallelismType.NO_PARALLELISM) {
            licenseManager.checkFeature(ORACLE_EXTENSIONS);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        return new FixedSplitSource(listSplits(
                JdbcIdentity.from(session),
                (JdbcTableHandle) table,
                getParallelismType(session),
                getMaxSplitsPerScan(session),
                dynamicFilter.getCurrentPredicate()));
    }

    private List<OracleSplit> listSplits(
            JdbcIdentity identity,
            JdbcTableHandle tableHandle,
            OracleParallelismType parallelismType,
            int maxSplits,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        if (parallelismType == NO_PARALLELISM || tableHandle.getGroupingSets().isPresent()) {
            return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty(), dynamicFilter));
        }

        if (parallelismType == PARTITIONS) {
            List<String> partitions = listPartitionsForTable(identity, tableHandle);

            if (partitions.isEmpty()) {
                // Table is not partitioned
                return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty(), dynamicFilter));
            }

            List<String> duplicatedPartitions = getDuplicates(partitions);
            verify(duplicatedPartitions.isEmpty(), "Partition names are not unique for table %s: %s", tableHandle, duplicatedPartitions);

            // Partition partitions into batches to limit total number of splits
            return Lists.partition(partitions, IntMath.divide(partitions.size(), maxSplits, CEILING)).stream()
                    .map(batch -> new OracleSplit(Optional.of(batch), Optional.empty(), dynamicFilter))
                    .collect(toImmutableList());
        }

        throw new IllegalArgumentException(format("Parallelism type %s is not supported", parallelismType));
    }

    private List<String> getDuplicates(List<String> values)
    {
        return values.stream()
                .collect(toImmutableMultiset()).entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableList());
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
