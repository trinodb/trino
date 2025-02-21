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
package io.trino.sql.dialect.ir;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Type;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;

public class IrDialect
        extends Dialect
{
    public static final String IR = "ir";
    public static final IrDialect IR_DIALECT = new IrDialect();

    public static final String TERMINAL = "terminal";

    private IrDialect()
    {
        super(IR);
    }

    @Override
    public String formatAttribute(String name, Object attribute)
    {
        if (!TERMINAL.equals(name) || !TRUE.equals(attribute)) {
            throw new TrinoException(IR_ERROR, format("the ir dialect does not support attribute %s = %s", name, attribute));
        }

        return "true";
    }

    @Override
    public Object parseAttribute(String name, String attribute)
    {
        if (!TERMINAL.equals(name) || !"true".equals(attribute)) {
            throw new TrinoException(IR_ERROR, format("the ir dialect does not support attribute %s = %s", name, attribute));
        }

        return true;
    }

    @Override
    public String formatType(Type type)
    {
        throw new UnsupportedOperationException("the ir dialect does not support any types");
    }

    @Override
    public Type parseType(String type)
    {
        throw new UnsupportedOperationException("the ir dialect does not support any types");
    }

    public static void terminalOperation(ImmutableMap.Builder<AttributeKey, Object> builder)
    {
        builder.put(new AttributeKey(IR, TERMINAL), true);
    }

    public static Map<AttributeKey, Object> terminalOperation()
    {
        return ImmutableMap.of(new AttributeKey(IR, TERMINAL), true);
    }
}
