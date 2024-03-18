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
package io.trino.plugin.varada.dispatcher;

import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.RowGroupDataFilter;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.metrics.PrintMetricsTimerTask;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_READ_OUT_OF_BOUNDS;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_UNRECOVERABLE_COLLECT_FAILED;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_UNRECOVERABLE_MATCH_FAILED;
import static java.util.Objects.requireNonNull;

@Singleton
public class ReadErrorHandler
{
    private static final Logger logger = Logger.get(ReadErrorHandler.class);
    private final WarmupDemoterService warmupDemoterService;
    private final RowGroupDataService rowGroupDataService;
    private final PrintMetricsTimerTask printMetricsTimerTask;

    @Inject
    public ReadErrorHandler(WarmupDemoterService warmupDemoterService,
                            RowGroupDataService rowGroupDataService,
                            PrintMetricsTimerTask printMetricsTimerTask)
    {
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.printMetricsTimerTask = requireNonNull(printMetricsTimerTask);
    }

    void handle(Throwable throwable, RowGroupData failedRowGroupData, QueryContext queryContext)
    {
        if (throwable instanceof TrinoException trinoException) {
            ErrorCode errorCode = trinoException.getErrorCode();
            if (errorCode.equals(VARADA_NATIVE_UNRECOVERABLE_ERROR.toErrorCode()) ||
                    errorCode.equals(VARADA_UNRECOVERABLE_MATCH_FAILED.toErrorCode()) ||
                    errorCode.equals(VARADA_UNRECOVERABLE_COLLECT_FAILED.toErrorCode())) {
                Set<WarmUpElement> queryContextWarmupElements = getQueryContextWarmupElements(queryContext);
                logger.warn("unrecoverable error: demoting rowGroupKey %s queryContextWarmupElements %s", failedRowGroupData.getRowGroupKey(), queryContextWarmupElements);
                warmupDemoterService.tryDemoteStart(List.of(new RowGroupDataFilter(failedRowGroupData.getRowGroupKey(), queryContextWarmupElements)));
            }
            else if (errorCode.equals(VARADA_NATIVE_READ_OUT_OF_BOUNDS.toErrorCode())) {
                Set<WarmUpElement> queryContextWarmupElements = getQueryContextWarmupElements(queryContext);
                Collection<WarmUpElement> allAsPermanentlyFailed = queryContextWarmupElements.stream().map(x -> WarmUpElement.builder(x).state(WarmUpElementState.FAILED_PERMANENTLY).build()).collect(Collectors.toSet());
                logger.warn("read out of bounds error: marking as failed rowGroupKey %s allAsPermanentlyFailed %s", failedRowGroupData.getRowGroupKey(), allAsPermanentlyFailed);
                rowGroupDataService.markAsFailed(failedRowGroupData.getRowGroupKey(), allAsPermanentlyFailed, failedRowGroupData.getPartitionKeys());
            }
            printMetricsTimerTask.print(false);
        }
    }

    private Set<WarmUpElement> getQueryContextWarmupElements(QueryContext queryContext)
    {
        return Streams.concat(queryContext.getNativeQueryCollectDataList().stream()
                                .map(QueryColumn::getWarmUpElementOptional)
                                .filter(Optional::isPresent)
                                .map(Optional::get),
                        queryContext.getMatchLeavesDFS().stream()
                                .map(QueryColumn::getWarmUpElementOptional)
                                .filter(Optional::isPresent)
                                .map(Optional::get))
                .collect(Collectors.toSet());
    }
}
