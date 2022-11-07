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
package io.trino.sql.planner.planprinter;

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class ValuePrinter
{
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final Session session;

    public ValuePrinter(Metadata metadata, FunctionManager functionManager, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.session = requireNonNull(session, "session is null");
    }

    public String castToVarchar(Type type, Object value)
    {
        try {
            return castToVarcharOrFail(type, value);
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
    }

    public String castToVarcharOrFail(Type type, Object value)
            throws OperatorNotFoundException
    {
        if (value == null) {
            return "NULL";
        }

        ResolvedFunction coercion = metadata.getCoercion(session, type, VARCHAR);
        Slice coerced = (Slice) new InterpretedFunctionInvoker(functionManager).invoke(coercion, session.toConnectorSession(), value);
        return coerced.toStringUtf8();
    }
}
