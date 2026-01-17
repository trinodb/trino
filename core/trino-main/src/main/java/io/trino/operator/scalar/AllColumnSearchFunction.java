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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public final class AllColumnSearchFunction
{
    private AllColumnSearchFunction() {}

    @Description("Search for a substring across all VARCHAR/CHAR columns (expanded during query planning)")
    @ScalarFunction("allcolumnsearch")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean allColumnSearch(@SqlType(StandardTypes.VARCHAR) Slice searchTerm)
    {
        throw new TrinoException(
                NOT_SUPPORTED,
                "allcolumnsearch() must be expanded during query planning and cannot be executed directly");
    }
}
