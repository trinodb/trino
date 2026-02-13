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
package io.trino.spi.type;

import java.math.BigDecimal;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SqlBigdecimal
{
    private final String valueAsString;

    public SqlBigdecimal(BigDecimal value)
    {
        this(value.toString());
    }

    public SqlBigdecimal(String valueAsString)
    {
        this.valueAsString = requireNonNull(valueAsString, "valueAsString is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SqlBigdecimal that)) {
            return false;
        }
        return Objects.equals(valueAsString, that.valueAsString);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(valueAsString);
    }

    @Override
    public String toString()
    {
        return valueAsString;
    }
}
