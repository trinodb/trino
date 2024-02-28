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
package io.trino.operator.table.json;

import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.ResolvedFunction;

import static java.util.Objects.requireNonNull;

public record JsonTableValueColumn(
        int outputIndex,
        ResolvedFunction function,
        IrJsonPath path,
        long emptyBehavior,
        int emptyDefaultInput, // channel number or -1 when default not specified
        long errorBehavior,
        int errorDefaultInput) // channel number or -1 when default not specified
        implements JsonTableColumn
{
    public JsonTableValueColumn
    {
        requireNonNull(function, "function is null");
        requireNonNull(path, "path is null");
    }
}
