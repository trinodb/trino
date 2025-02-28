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

import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;

import java.util.List;

public interface SqlOperation
        // TODO sealed interface?
{
    /**
     * Change the operations result.
     * The new result type must be the same as the returned type of the operation.
     */
    Operation withResult(Operation.Result newResult);

    /**
     * Change the operations regions.
     * The newRegions list must be the size of operations regions.
     */
    Operation withRegions(List<Region> newRegions);
}
