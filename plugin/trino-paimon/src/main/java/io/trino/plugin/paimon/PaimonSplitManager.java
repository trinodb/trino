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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import jakarta.annotation.PreDestroy;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PaimonSplitManager
        implements ConnectorSplitManager
{
    private final PaimonCatalog paimonCatalog;
    private final PaimonConnectorStats stats;

    @Inject
    public PaimonSplitManager(PaimonMetadataFactory paimonMetadataFactory, PaimonConnectorStats stats)
    {
        this.paimonCatalog = requireNonNull(paimonMetadataFactory, "trinoMetadataFactory is null").create().catalog();
        this.stats = requireNonNull(stats, "stats is null");
    }

    @PreDestroy
    public void destroy()
    {
        // No resources to cleanup currently
        // Add executor shutdown here if needed in future
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        requireNonNull(session, "session is null");
        requireNonNull(dynamicFilterColumns, "dynamicFilterColumns is null");
        requireNonNull(constraint, "constraint is null");
        return getSplits(getTableHandle(table), session, DynamicFilter.EMPTY);
    }

    static PaimonTableHandle getTableHandle(ConnectorTableHandle tableHandle)
    {
        if (!(requireNonNull(tableHandle, "tableHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon split planning requires PaimonTableHandle, got: "
                    + tableHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    static PaimonTableHandle getTableFunctionHandle(ConnectorTableFunctionHandle functionHandle)
    {
        if (!(requireNonNull(functionHandle, "functionHandle is null") instanceof PaimonTableHandle paimonTableHandle)) {
            throw new IllegalStateException("Paimon table function split planning requires PaimonTableHandle, got: "
                    + functionHandle.getClass().getName());
        }
        return paimonTableHandle;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        requireNonNull(session, "session is null");
        PaimonTableHandle tableHandle = getTableFunctionHandle(function);
        return getSplits(tableHandle, session, DynamicFilter.EMPTY);
    }

    protected ConnectorSplitSource getSplits(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            DynamicFilter dynamicFilter)
    {
        TupleDomain<PaimonColumnHandle> effectivePredicate = effectivePredicate(tableHandle, dynamicFilter);
        if (isEmptySplit(effectivePredicate, tableHandle)) {
            return new ClassLoaderSafeConnectorSplitSource(
                    emptySplitSource(tableHandle),
                    PaimonSplitManager.class.getClassLoader());
        }

        Duration dynamicFilteringWaitTimeout = PaimonSessionProperties.getDynamicFilteringWaitTimeout(session);

        if (!canApplyDynamicFilter(tableHandle) || dynamicFilteringWaitTimeout.toMillis() == 0 || !dynamicFilter.isAwaitable()) {
            return planSplits(tableHandle, session, effectivePredicate);
        }

        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                tableHandle,
                session,
                paimonCatalog,
                dynamicFilter,
                dynamicFilteringWaitTimeout);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
    }

    static TupleDomain<PaimonColumnHandle> effectivePredicate(PaimonTableHandle tableHandle, DynamicFilter dynamicFilter)
    {
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        TupleDomain<PaimonColumnHandle> staticPredicate = requireNonNull(tableHandle, "tableHandle is null").getFilter();
        // Runtime dynamic filter domains can be evaluated by Paimon manifest stats pruning
        // against evolved/dense stats rows that do not contain every filtered column. Keep
        // static predicate pushdown, and only use dynamic filtering for the empty build-side
        // case where split planning can be skipped entirely.
        if (dynamicFilter.getCurrentPredicate().isNone()) {
            return TupleDomain.none();
        }

        if (!canApplyDynamicFilter(tableHandle)) {
            return staticPredicate;
        }

        return staticPredicate;
    }

    static boolean canApplyDynamicFilter(PaimonTableHandle tableHandle)
    {
        return requireNonNull(tableHandle, "tableHandle is null").getLimit().isEmpty();
    }

    private ConnectorSplitSource planSplits(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            TupleDomain<PaimonColumnHandle> predicate)
    {
        if (isEmptySplit(predicate, tableHandle)) {
            return new ClassLoaderSafeConnectorSplitSource(
                    emptySplitSource(tableHandle),
                    PaimonSplitManager.class.getClassLoader());
        }

        try {
            Catalog catalog = paimonCatalog.forSession(session);
            Table table = PaimonTableHandle.schemaAwareReadTable(
                    tableHandle.tableWithDynamicOptions(catalog, session),
                    !tableHandle.usesHistoricalReadSchema(session));
            ReadBuilder readBuilder = table.newReadBuilder();
            pushPredicate(readBuilder, table, predicate);
            List<Split> splits = planScan(readBuilder);
            double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);
            List<PaimonSplit> paimonSplits = toPaimonSplits(splits, minimumSplitWeight);
            PaimonSplitSource splitSource = new PaimonSplitSource(paimonSplits, tableHandle.getLimit());

            stats.incrementSplitCount();
            for (PaimonSplit paimonSplit : paimonSplits) {
                stats.addSplitRowCount(paimonSplit.rowCount() != null ? paimonSplit.rowCount() : 0);
                if (paimonSplit.weight() != null) {
                    stats.addSplitWeight(paimonSplit.weight());
                }
            }
            return new ClassLoaderSafeConnectorSplitSource(splitSource, PaimonSplitManager.class.getClassLoader());
        }
        catch (UnsupportedOperationException e) {
            throw unsupportedReadOperation(tableHandle, e);
        }
        catch (RuntimeException e) {
            throw splitPlanningException(tableHandle, e);
        }
    }

    static TrinoException unsupportedReadOperation(PaimonTableHandle tableHandle, UnsupportedOperationException cause)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(cause, "cause is null");

        String message = tableHandle.hasIncrementalReadMode()
                ? "Paimon system.table_changes uses features which are not supported by the Trino connector"
                : "Paimon table read uses features which are not supported by the Trino connector";
        return new TrinoException(NOT_SUPPORTED, messageWithCauseDetail(message, cause), cause);
    }

    static RuntimeException splitPlanningException(PaimonTableHandle tableHandle, Exception cause)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(cause, "cause is null");

        Throwable planningFailure = firstRecognizedSplitPlanningFailure(cause);
        if (planningFailure instanceof TrinoException trinoException) {
            return trinoException;
        }

        String message = tableHandle.hasIncrementalReadMode()
                ? "Failed to plan Paimon table_changes splits"
                : "Failed to plan Paimon splits";
        if (planningFailure instanceof UnsupportedOperationException unsupportedOperationException) {
            return unsupportedReadOperation(tableHandle, unsupportedOperationException);
        }
        if (planningFailure instanceof UncheckedIOException uncheckedIOException) {
            IOException ioException = uncheckedIOException.getCause();
            return new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, messageWithCauseDetail(message, ioException), ioException);
        }
        if (planningFailure instanceof IOException ioException) {
            return new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, messageWithCauseDetail(message, ioException), ioException);
        }
        return new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, messageWithCauseDetail(message, cause), cause);
    }

    private static String messageWithCauseDetail(String message, Throwable cause)
    {
        requireNonNull(message, "message is null");
        requireNonNull(cause, "cause is null");

        String detail = cause.getMessage();
        if (detail == null || detail.isBlank()) {
            detail = cause.getClass().getSimpleName();
        }
        return message + ": " + detail;
    }

    private static Throwable firstRecognizedSplitPlanningFailure(Throwable cause)
    {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = cause;
        while (current != null && visited.add(current)) {
            if (current instanceof TrinoException ||
                    current instanceof UnsupportedOperationException ||
                    current instanceof UncheckedIOException ||
                    current instanceof IOException) {
                return current;
            }
            current = current.getCause();
        }
        return cause;
    }

    static void pushLimit(ReadBuilder readBuilder, PaimonTableHandle tableHandle)
    {
        requireNonNull(readBuilder, "readBuilder is null");
        OptionalLong limit = requireNonNull(tableHandle, "tableHandle is null").getLimit();
        if (limit.isPresent() && limit.orElseThrow() <= Integer.MAX_VALUE) {
            readBuilder.withLimit(toIntExact(limit.orElseThrow()));
        }
    }

    static void pushPredicate(ReadBuilder readBuilder, Table table, TupleDomain<PaimonColumnHandle> predicate)
    {
        requireNonNull(readBuilder, "readBuilder is null");
        requireNonNull(table, "table is null");
        requireNonNull(predicate, "predicate is null");

        PaimonRowRangeExtractor.extractRowIdRanges(predicate).ifPresent(readBuilder::withRowRanges);

        TupleDomain<PaimonColumnHandle> pushdownPredicate = PaimonRowRangeExtractor.removeRowIdPredicate(predicate);
        Optional<Predicate> paimonPredicate = new PaimonFilterConverter(
                PaimonTableHandle.effectiveReadRowType(table)).convert(pushdownPredicate);
        paimonPredicate.ifPresent(readBuilder::withFilter);
    }

    static List<Split> planScan(ReadBuilder readBuilder)
    {
        requireNonNull(readBuilder, "readBuilder is null");
        // Keep manifest statistics available while Paimon plans the scan so file-level
        // predicate pruning can use them. Dropping statistics before plan() disables pruning.
        return readBuilder.newScan().plan().splits();
    }

    static double calculateSplitWeight(Split split, long maxRowCount, double minimumSplitWeight)
    {
        requireNonNull(split, "split is null");
        return calculateSplitWeight(splitWeightRowCount(split), maxRowCount, minimumSplitWeight);
    }

    static double calculateSplitWeight(long rowCount, long maxRowCount, double minimumSplitWeight)
    {
        checkMinimumSplitWeight(minimumSplitWeight);
        checkArgument(rowCount >= 0, "split row count must be non-negative");
        if (maxRowCount <= 0 || rowCount <= 0) {
            return minimumSplitWeight;
        }
        return Math.min(Math.max((double) rowCount / maxRowCount, minimumSplitWeight), 1.0);
    }

    static List<PaimonSplit> toPaimonSplits(List<Split> splits, double minimumSplitWeight)
    {
        requireNonNull(splits, "splits is null");
        checkMinimumSplitWeight(minimumSplitWeight);

        long[] rowCounts = new long[splits.size()];
        long maxRowCount = 0;
        int index = 0;
        for (Split split : splits) {
            requireNonNull(split, "splits contains null split");
            long rowCount = splitWeightRowCount(split);
            checkArgument(rowCount >= 0, "split row count must be non-negative");
            rowCounts[index] = rowCount;
            maxRowCount = Math.max(maxRowCount, rowCount);
            index++;
        }

        List<PaimonSplit> paimonSplits = new ArrayList<>(splits.size());
        index = 0;
        for (Split split : splits) {
            long rowCount = rowCounts[index];
            paimonSplits.add(PaimonSplit.fromSplit(
                    split,
                    calculateSplitWeight(rowCount, maxRowCount, minimumSplitWeight),
                    rowCount));
            index++;
        }
        return paimonSplits;
    }

    private static void checkMinimumSplitWeight(double minimumSplitWeight)
    {
        checkArgument(Double.isFinite(minimumSplitWeight) && minimumSplitWeight > 0 && minimumSplitWeight <= 1,
                "minimumSplitWeight must be in the range (0, 1]");
    }

    static long splitWeightRowCount(Split split)
    {
        requireNonNull(split, "split is null");
        return split.mergedRowCount().orElseGet(split::rowCount);
    }

    static PaimonSplitSource emptySplitSource(PaimonTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return new PaimonSplitSource(List.of(), tableHandle.getLimit());
    }

    static boolean isEmptySplit(TupleDomain<PaimonColumnHandle> predicate, PaimonTableHandle tableHandle)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(tableHandle, "tableHandle is null");
        return predicate.isNone() || (tableHandle.getLimit().isPresent() && tableHandle.getLimit().orElseThrow() == 0);
    }
}
