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
package io.trino.operator.function;

import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed;
import static java.util.Objects.requireNonNull;

public class TableFunctionWorkProcessor
        implements WorkProcessor.Transformation<List<Optional<Page>>, Page>
{
    private final TableFunctionDataProcessor tableFunctionDataProcessor;

    public TableFunctionWorkProcessor(TableFunctionDataProcessor tableFunctionDataProcessor)
    {
        this.tableFunctionDataProcessor = requireNonNull(tableFunctionDataProcessor, "tableFunctionDataProcessor is null");
    }

    @Override
    public WorkProcessor.TransformationState<Page> process(List<Optional<Page>> inputPage)
    {
        TableFunctionProcessorState state = tableFunctionDataProcessor.process(inputPage);
        boolean functionGotNoData = inputPage == null;
        if (state == FINISHED) {
            return WorkProcessor.TransformationState.finished();
        }
        if (state instanceof Blocked blocked) {
            return WorkProcessor.TransformationState.blocked(toListenableFuture(blocked.getFuture()));
        }
        Processed processed = (Processed) state;
        if (!functionGotNoData && processed.getResult() == null && processed.isUsedInput()) {
            return WorkProcessor.TransformationState.needsMoreData();
        }
        if (processed.getResult() != null) {
            return WorkProcessor.TransformationState.ofResult(processed.getResult(), processed.isUsedInput() && !functionGotNoData);
        }
        if (functionGotNoData) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "When function got no input, it should either produce output or return Blocked state");
        }
        return WorkProcessor.TransformationState.blocked(immediateFuture(null));
    }
}
