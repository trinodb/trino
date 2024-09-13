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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Captures;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import io.trino.split.PageSourceManager;
import io.trino.split.PageSourceProvider;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.getMaterializeTableMaxActualRowCount;
import static io.trino.SystemSessionProperties.getMaterializeTableMaxEstimatedRowCount;
import static io.trino.SystemSessionProperties.getMaterializeTableTimeout;
import static io.trino.SystemSessionProperties.isMaterializeTableEnabled;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract sealed class AbstractMaterializeTableScan<T extends PlanNode>
        implements Rule<T>
        permits MaterializeFilteredTableScan, MaterializeTableScan
{
    private static final int MAX_SPLITS = 10_000;

    protected final Logger log = Logger.get(getClass());
    protected final PlannerContext plannerContext;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final ExecutorService executor;
    private final HostAddress currentNode;

    protected AbstractMaterializeTableScan(
            PlannerContext plannerContext,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            InternalNodeManager nodeManager,
            ExecutorService executor)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.currentNode = nodeManager.getCurrentNode().getHostAndPort();
    }

    @Override
    public final boolean isEnabled(Session session)
    {
        return isMaterializeTableEnabled(session) &&
                getMaterializeTableMaxEstimatedRowCount(session) > 0 &&
                getMaterializeTableMaxActualRowCount(session) > 0 &&
                getMaterializeTableTimeout(session).toMillis() > 0;
    }

    @Override
    public final Result apply(T node, Captures captures, Context context)
    {
        int maxRows = getMaterializeTableMaxEstimatedRowCount(context.getSession());
        PlanNodeStatsEstimate stats = context.getStatsProvider().getStats(node);
        if (stats.getOutputRowCount() > maxRows) {
            return Result.empty();
        }

        return apply(node, captures, context.getSession());
    }

    protected abstract Result apply(T node, Captures captures, Session session);

    protected Optional<List<Expression>> materializeTable(Session session, TableScanNode tableScan, Constraint constraint)
    {
        TableHandle table = tableScan.getTable();
        List<ColumnHandle> columns = tableScan.getOutputSymbols().stream()
                .map(symbol -> tableScan.getAssignments().get(symbol))
                .toList();
        List<Type> types = tableScan.getOutputSymbols().stream()
                .map(Symbol::type)
                .toList();

        Future<Optional<List<Expression>>> future = executor.submit(() -> {
            try (var _ = new SetThreadName("MaterializeTableScan-%s", table.catalogHandle())) {
                return doMaterializeTable(session, table, columns, types, constraint);
            }
        });

        Duration timeout = getMaterializeTableTimeout(session);
        try {
            return future.get(timeout.toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new RuntimeException("interrupted", e);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof TableTooLargeException) {
                log.warn("Table too large to materialize: %s", table);
            }
            else {
                log.warn(e.getCause(), "Failed to materialize table: %s", table);
            }
        }
        catch (TimeoutException e) {
            log.warn("Timed out while materializing table: %s", table);
            future.cancel(true);
        }
        return Optional.empty();
    }

    private Optional<List<Expression>> doMaterializeTable(Session session, TableHandle table, List<ColumnHandle> columns, List<Type> types, Constraint constraint)
            throws IOException
    {
        SplitSource splitSource = splitManager.getSplits(session, session.getQuerySpan(), table, DynamicFilter.EMPTY, constraint);
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }
            List<Split> batch = getFutureValue(splitSource.getNextBatch(1000)).getSplits();
            for (Split split : batch) {
                if (!split.isRemotelyAccessible() && !split.getAddresses().contains(currentNode)) {
                    return Optional.empty();
                }
            }
            splits.addAll(batch);
            if (splits.size() > MAX_SPLITS) {
                return Optional.empty();
            }
        }

        PageSourceProvider pageSourceProvider = pageSourceManager.createPageSourceProvider(table.catalogHandle());
        int maxRows = getMaterializeTableMaxActualRowCount(session);
        int rowCount = 0;
        ImmutableList.Builder<Expression> rows = ImmutableList.builder();
        for (Split split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, split, table, columns, DynamicFilter.EMPTY)) {
                while (!pageSource.isFinished()) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedIOException();
                    }
                    Page page = pageSource.getNextPage();
                    if (page != null && page.getPositionCount() > 0) {
                        rowCount += page.getPositionCount();
                        if (rowCount > maxRows) {
                            throw new TableTooLargeException();
                        }
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            rows.add(toRow(page, i, types));
                        }
                    }
                }
            }
        }
        return Optional.of(rows.build());
    }

    private static Row toRow(Page page, int position, List<Type> types)
    {
        ImmutableList.Builder<Expression> row = ImmutableList.builder();
        for (int i = 0; i < page.getChannelCount(); i++) {
            Type type = types.get(i);
            Block block = page.getBlock(i);
            Object value = readNativeValue(type, block, position);
            row.add(new Constant(type, value));
        }
        return new Row(row.build());
    }

    private static final class TableTooLargeException
            extends IOException {}
}
