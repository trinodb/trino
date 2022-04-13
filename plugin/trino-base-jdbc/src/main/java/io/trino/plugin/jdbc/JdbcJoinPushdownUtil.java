/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;

import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.plugin.jdbc.JdbcJoinPushdownSessionProperties.getJoinPushdownAutomaticJoinToTablesRatio;
import static io.trino.plugin.jdbc.JdbcJoinPushdownSessionProperties.getJoinPushdownAutomaticMaxTableSize;
import static io.trino.plugin.jdbc.JdbcJoinPushdownSessionProperties.getJoinPushdownStrategy;
import static java.lang.String.format;

public final class JdbcJoinPushdownUtil
{
    private static final Logger LOG = Logger.get(JdbcJoinPushdownUtil.class);

    private JdbcJoinPushdownUtil() {}

    public static Optional<PreparedQuery> implementJoinCostAware(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            JoinStatistics statistics,
            Supplier<Optional<PreparedQuery>> delegate)
    {
        // Calling out to super.implementJoin() before shouldImplementJoinPushdownBasedOnStats() so we can return quickly if we know we should not push down, even without
        // analyzing table statistics. Getting table statistics can be expensive and we want to avoid that if possible.
        Optional<PreparedQuery> result = delegate.get();
        if (result.isEmpty()) {
            return Optional.empty();
        }

        JoinPushdownStrategy joinPushdownStrategy = getJoinPushdownStrategy(session);
        switch (joinPushdownStrategy) {
            case EAGER:
                return result;

            case AUTOMATIC:
                if (shouldPushDownJoinCostAware(session, joinType, leftSource, rightSource, statistics)) {
                    return result;
                }
                return Optional.empty();
        }
        throw new IllegalArgumentException("Unsupported joinPushdownStrategy: " + joinPushdownStrategy);
    }

    /**
     * Common implementation of AUTOMATIC join pushdown strategy to by used in SEP Jdbc connectors
     */
    public static boolean shouldPushDownJoinCostAware(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            JoinStatistics statistics)
    {
        long maxTableSizeBytes = getJoinPushdownAutomaticMaxTableSize(session).map(DataSize::toBytes).orElse(Long.MAX_VALUE);

        String joinSignature = "";
        if (LOG.isDebugEnabled()) {
            joinSignature = diagnosticsJoinSignature(joinType, leftSource, rightSource);
        }

        if (statistics.getLeftStatistics().isEmpty()) {
            logNoPushdown(joinSignature, "left stats empty");
            return false;
        }

        double leftDataSize = statistics.getLeftStatistics().get().getDataSize();
        if (leftDataSize > maxTableSizeBytes) {
            logNoPushdown(joinSignature, () -> "left size " + leftDataSize + " > " + maxTableSizeBytes);
            return false;
        }

        if (statistics.getRightStatistics().isEmpty()) {
            logNoPushdown(joinSignature, "right stats empty");
            return false;
        }

        double rightDataSize = statistics.getRightStatistics().get().getDataSize();
        if (rightDataSize > maxTableSizeBytes) {
            logNoPushdown(joinSignature, () -> "right size " + rightDataSize + " > " + maxTableSizeBytes);
            return false;
        }

        if (statistics.getJoinStatistics().isEmpty()) {
            logNoPushdown(joinSignature, "join stats empty");
            return false;
        }

        double joinDataSize = statistics.getJoinStatistics().get().getDataSize();
        if (joinDataSize < getJoinPushdownAutomaticJoinToTablesRatio(session) * (leftDataSize + rightDataSize)) {
            // This is poor man's estimation if it makes more sense to perform join in source database or SEP.
            // The assumption here is that cost of performing join in source database is less than or equal to cost of join in SEP.
            // We resolve tie for pessimistic case (both join costs equal) on cost of sending the data from source database to SEP.
            LOG.debug("triggering join pushdown for %s", joinSignature);
            return true;
        }
        logNoPushdown(joinSignature, () ->
                "joinDataSize " + joinDataSize + " >= " +
                        getJoinPushdownAutomaticJoinToTablesRatio(session) +
                        " * (leftDataSize " + leftDataSize +
                        " + rightDataSize " + rightDataSize + ") = " + getJoinPushdownAutomaticJoinToTablesRatio(session) * (leftDataSize + rightDataSize));
        return false;
    }

    private static String diagnosticsJoinSignature(
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource)
    {
        return format("%s JOIN(%s; %s)", joinType, leftSource.getQuery(), rightSource.getQuery());
    }

    private static void logNoPushdown(String joinSignature, String reason)
    {
        logNoPushdown(joinSignature, () -> reason);
    }

    private static void logNoPushdown(String joinSignature, Supplier<String> reason)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("skipping join pushdown for %s; %s", joinSignature, reason.get());
        }
    }
}
