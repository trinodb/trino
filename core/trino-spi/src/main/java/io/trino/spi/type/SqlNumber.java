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

public final class SqlNumber
{
    private final TrinoNumber.AsBigDecimal value;

    // TODO remove redundant constructors

    public SqlNumber(String value)
    {
        this(new BigDecimal(value));
    }

    public SqlNumber(BigDecimal value)
    {
        this(new TrinoNumber.BigDecimalValue(value));
    }

    public SqlNumber(TrinoNumber.AsBigDecimal value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    public TrinoNumber.AsBigDecimal value()
    {
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SqlNumber that)) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(value);
    }

    @Override
    public String toString()
    {
        return value.toString();
    }
}
