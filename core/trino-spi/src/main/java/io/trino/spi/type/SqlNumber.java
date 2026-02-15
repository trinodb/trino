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
import java.util.Base64;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class SqlNumber
{
    private final String stringified;
    private final String base64Encoded;

    public SqlNumber(BigDecimal value)
    {
        this(value.toString());
    }

    public SqlNumber(String value)
    {
        stringified = value;
        base64Encoded = Base64.getEncoder().encodeToString(stringified.getBytes(UTF_8));
    }

    public String stringified()
    {
        return stringified;
    }

    public String base64Encoded()
    {
        return base64Encoded;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SqlNumber that)) {
            return false;
        }
        return Objects.equals(stringified, that.stringified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(stringified);
    }

    @Override
    public String toString()
    {
        return stringified;
    }
}
