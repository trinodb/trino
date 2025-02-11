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
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static java.util.Objects.requireNonNull;

public class TableFunctionWorkProcessor
        implements WorkProcessor.Transformation<TableFunctionWorkProcessor.TableFunctionProcessorInput, Page>
{
    private final TableFunctionDataProcessor tableFunction;

    public TableFunctionWorkProcessor(TableFunctionDataProcessor tableFunction)
    {
        this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
    }

    @Override
    public WorkProcessor.TransformationState<Page> process(TableFunctionWorkProcessor.TableFunctionProcessorInput inputPages)
    {
        if (inputPages == null) {
            return WorkProcessor.TransformationState.finished();
        }
        TableFunctionProcessorState state = tableFunction.process(inputPages.result());
        boolean functionGotNoData = inputPages == null;
        if (state == FINISHED) {
            return WorkProcessor.TransformationState.finished();
        }
        if (state instanceof TableFunctionProcessorState.Blocked blocked) {
            return WorkProcessor.TransformationState.blocked(toListenableFuture(blocked.getFuture()));
        }
        TableFunctionProcessorState.Processed processed = (TableFunctionProcessorState.Processed) state;
        if (processed.getResult() != null) {
            return WorkProcessor.TransformationState.ofResult(inputPages.passthroughProcessor().apply(processed.getResult()));
        }
        if (functionGotNoData) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "When function got no input, it should either produce output or return Blocked state");
        }
        return WorkProcessor.TransformationState.blocked(immediateFuture(null));
    }

    record TableFunctionProcessorInput(List<Optional<Page>> result, Function<Page, Page> passthroughProcessor) {}
}
