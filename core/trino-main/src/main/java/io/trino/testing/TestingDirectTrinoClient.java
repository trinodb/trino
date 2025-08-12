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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.direct.DirectTrinoClient;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryManagerConfig;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.ResultQueryInfo;
import io.trino.server.SessionContext;
import io.trino.server.protocol.ProtocolUtil;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.util.MoreLists.mappedCopy;
import static java.util.Objects.requireNonNull;

public class TestingDirectTrinoClient
{
    private final DirectTrinoClient directTrinoClient;

    public TestingDirectTrinoClient(DispatchManager dispatchManager, QueryManager queryManager, QueryManagerConfig queryManagerConfig, DirectExchangeClientSupplier directExchangeClientSupplier, BlockEncodingSerde blockEncodingSerde)
    {
        directTrinoClient = new DirectTrinoClient(dispatchManager, queryManager, queryManagerConfig, directExchangeClientSupplier, blockEncodingSerde);
    }

    public Result execute(Session session, @Language("SQL") String sql)
    {
        return execute(SessionContext.fromSession(session), sql);
    }

    public Result execute(SessionContext sessionContext, @Language("SQL") String sql)
    {
        MaterializedQueryResultsListener queryResultsListener = new MaterializedQueryResultsListener();
        DispatchQuery dispatchQuery = directTrinoClient.execute(sessionContext, sql, queryResultsListener);
        return new Result(dispatchQuery.getQueryId(), () -> toMaterializedRows(dispatchQuery, queryResultsListener.columnTypes(), queryResultsListener.columnNames(), queryResultsListener.pages()));
    }

    private static MaterializedResult toMaterializedRows(DispatchQuery dispatchQuery, List<Type> columnTypes, List<String> columnNames, List<Page> pages)
    {
        QueryInfo queryInfo = dispatchQuery.getFullQueryInfo();

        if (queryInfo.getState() != FINISHED) {
            if (queryInfo.getFailureInfo() == null) {
                throw new QueryFailedException(queryInfo.getQueryId(), "Query failed without failure info");
            }
            RuntimeException remoteException = queryInfo.getFailureInfo().toException();
            throw new QueryFailedException(queryInfo.getQueryId(), Optional.ofNullable(remoteException.getMessage()).orElseGet(remoteException::toString), remoteException);
        }

        if (pages.isEmpty() && columnTypes == null) {
            // the query did not produce any output
            return new MaterializedResult(
                    Optional.of(dispatchQuery.getSession()),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    queryInfo.getSetSessionProperties(),
                    queryInfo.getResetSessionProperties(),
                    Optional.ofNullable(queryInfo.getUpdateType()),
                    OptionalLong.empty(),
                    mappedCopy(queryInfo.getWarnings(), ProtocolUtil::toClientWarning),
                    Optional.of(ProtocolUtil.toStatementStats(new ResultQueryInfo(queryInfo))));
        }

        List<MaterializedRow> materializedRows = toMaterializedRows(columnTypes, pages);

        OptionalLong updateCount = OptionalLong.empty();
        if (queryInfo.getUpdateType() != null && materializedRows.size() == 1 && columnTypes.size() == 1 && columnTypes.get(0).equals(BIGINT)) {
            Number value = (Number) materializedRows.get(0).getField(0);
            if (value != null) {
                updateCount = OptionalLong.of(value.longValue());
            }
        }

        return new MaterializedResult(
                Optional.of(dispatchQuery.getSession()),
                materializedRows,
                columnTypes,
                columnNames,
                Optional.empty(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                Optional.ofNullable(queryInfo.getUpdateType()),
                updateCount,
                mappedCopy(queryInfo.getWarnings(), ProtocolUtil::toClientWarning),
                Optional.of(ProtocolUtil.toStatementStats(new ResultQueryInfo(queryInfo))));
    }

    public static List<MaterializedRow> toMaterializedRows(List<Type> types, List<Page> pages)
    {
        ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        for (Page page : pages) {
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            }
        }
        return rows.build();
    }

    public record Result(QueryId queryId, Supplier<MaterializedResult> result)
    {
        public Result
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(result, "result is null");
        }
    }

    private static class MaterializedQueryResultsListener
            implements DirectTrinoClient.QueryResultsListener
    {
        private List<String> columnNames;
        private List<Type> columnTypes;
        private List<Page> pages = new ArrayList<>();

        @Override
        public void setOutputColumns(List<String> columnNames, List<Type> columnTypes)
        {
            this.columnNames = requireNonNull(columnNames, "columnNames is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        }

        @Override
        public void consumeOutputPage(Page page)
        {
            pages.add(page);
        }

        public List<String> columnNames()
        {
            return columnNames;
        }

        public List<Type> columnTypes()
        {
            return columnTypes;
        }

        public List<Page> pages()
        {
            return pages;
        }
    }
}
