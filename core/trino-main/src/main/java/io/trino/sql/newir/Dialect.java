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
package io.trino.sql.newir;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.newir.FormatOptions.isValidIdentifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class Dialect
{
    private final String name;

    public Dialect(String name)
    {
        requireNonNull(name, "name is null");
        validateDialectName(name);
        this.name = name;
    }

    public static void validateDialectName(String name)
    {
        if (!isValidIdentifier(name)) {
            throw new TrinoException(IR_ERROR, format("invalid dialect name: \"%s\"", name));
        }
    }

    public String name()
    {
        return name;
    }

    public abstract String formatAttribute(String name, Object attribute);

    public abstract Object parseAttribute(String name, String attribute);

    public abstract String formatType(Type type);

    public abstract Type parseType(String type);
}
