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
package io.trino.sql.dialect.trino.operation;

import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.CteReuse;

import java.util.Optional;

public interface SqlRelationalOperation
        // TODO sealed interface?
        extends SqlOperation
{
    /**
     * Rebase operation onto baseOperation, using mapping to update field references.
     * The returned operation has a new result name and possibly a different returned type than the original operation.
     * Optional.empty is returned when some field reference cannot be remapped.
     * TODO support rebasing with multiple source: rebase one source at a time.
     * // TODO index is the index of where the baseOperation should go in this operations children
     */
    Optional<Operation> rebase(Operation baseOperation, int sourceIndex, CteReuse.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator);
    // TODO if mapping is identity, just change the input. Operations should implement `withArguments()`, similar to `withResult()`
    // TODO with partial rebase, we miss the attributes of the unchanged sources!
}
