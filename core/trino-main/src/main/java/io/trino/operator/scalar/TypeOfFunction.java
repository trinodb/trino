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
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;

@Description("Textual representation of expression type")
@ScalarFunction("typeof")
public final class TypeOfFunction
{
    private TypeOfFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlNullable @SqlType("T") Object value)
    {
        return utf8Slice(type.getDisplayName());
    }
}
