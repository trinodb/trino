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
import io.trino.sql.newir.Operation.AttributeKey;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.Dialect.TRINO;

public class FormatOptions
{
    public static final String INDENT = "    ";

    private FormatOptions() {}

    public static void validateVersion(int version)
    {
        if (version != 1) {
            throw new TrinoException(IR_ERROR, "invalid format version: " + version);
        }
    }

    public static String formatName(int version, Operation operation)
    {
        if (version == 1 && operation.dialect().equals(TRINO)) {
            return operation.name();
        }
        return operation.dialect() + "." + operation.name();
    }

    public static String formatName(int version, AttributeKey key)
    {
        if (version == 1 && key.dialect().equals(TRINO)) {
            return key.name();
        }
        return key.dialect() + "." + key.name();
    }

    public static String formatType(int version, Type type)
    {
        if (version == 1 && type.dialect().equals(TRINO)) {
            return type.dialectType().toString();
        }
        return type.dialect() + "." + type.dialectType().toString();
    }
}
