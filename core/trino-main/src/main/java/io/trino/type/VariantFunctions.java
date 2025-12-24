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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;

public final class VariantFunctions
{
    private VariantFunctions() {}

    /**
     * Encodes a Variant into its binary representation.
     * This is a useful bridge function importing raw variant data, but generally should not be used directly.
     */
    @ScalarFunction(hidden = true)
    @SqlType(StandardTypes.VARIANT)
    public static Variant decodeVariant(@SqlType(StandardTypes.VARBINARY) Slice metadata, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        return Variant.from(Metadata.from(metadata), value);
    }

    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean variantIsNull(@SqlType(StandardTypes.VARIANT) Variant value)
    {
        return value.isNull();
    }
}
