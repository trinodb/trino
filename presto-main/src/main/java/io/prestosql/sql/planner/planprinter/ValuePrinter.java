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
package io.prestosql.sql.planner.planprinter;

import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class ValuePrinter
{
    private final Metadata metadata;
    private final Session session;

    public ValuePrinter(Metadata metadata, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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

        Signature coercion = metadata.getCoercion(type, VARCHAR);
        Slice coerced = (Slice) new InterpretedFunctionInvoker(metadata).invoke(coercion, session.toConnectorSession(), value);
        return coerced.toStringUtf8();
    }
}
