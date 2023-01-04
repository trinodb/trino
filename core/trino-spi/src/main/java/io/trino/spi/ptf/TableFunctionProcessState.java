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
package io.trino.spi.ptf;

import io.trino.spi.Experimental;
import io.trino.spi.Page;

import java.util.concurrent.CompletableFuture;

import static io.trino.spi.ptf.Preconditions.checkArgument;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.BLOCKED;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.FORWARD;

/**
 * The result of processing input by {@link TableFunctionDataProcessor} or {@link TableFunctionSplitProcessor}.
 * It can optionally include a {@link Page} of output data. The returned {@link Page} should consist of:
 * - proper columns produced by the table function
 * - one column of type {@code BIGINT} for each table function's input table having the pass-through property (see {@link TableArgumentSpecification#isPassThroughColumns}),
 * in order of the corresponding argument specifications. Entries in these columns are the indexes of input rows (from partition start) to be attached to output,
 * or null to indicate that a row of nulls should be attached instead of an input row. The indexes are validated to be within the portion of the partition
 * provided to the function so far.
 * Note: when the input is empty, the only valid index value is null, because there are no input rows that could be attached to output. In such case, for performance
 * reasons, the validation of indexes is skipped, and all pass-through columns are filled with nulls.
 */
@Experimental(eta = "2023-03-31")
public record TableFunctionProcessState(Type type, boolean usedData, TableFunctionResult result, CompletableFuture<Void> blockedFuture)
{
    public TableFunctionProcessState
    {
        checkArgument(type != FORWARD || (usedData || result != null), "Process state is FORWARD but table function neither used data nor produced output");
        checkArgument(type == FORWARD || (!usedData && result == null), "Table function used data or produced output but process state is " + type);
        checkArgument(type != BLOCKED || blockedFuture != null, "Table function returned no blockedFuture but process state is BLOCKED");
        checkArgument(type == BLOCKED || blockedFuture == null, "Table function returned blockedFuture but process state is " + type);
    }

    public enum Type
    {
        FORWARD,
        BLOCKED,
        FINISHED
    }

    public record TableFunctionResult(Page page)
    {
    }
}
