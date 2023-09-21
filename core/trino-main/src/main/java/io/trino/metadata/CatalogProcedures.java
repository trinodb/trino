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
package io.trino.metadata;

import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.PROCEDURE_NOT_FOUND;
import static io.trino.spi.procedure.Procedure.Argument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class CatalogProcedures
{
    private final Map<SchemaTableName, Procedure> procedures;

    public CatalogProcedures(Collection<Procedure> procedures)
    {
        requireNonNull(procedures, "procedures is null");
        procedures.forEach(CatalogProcedures::validateProcedure);

        this.procedures = Maps.uniqueIndex(
                procedures,
                procedure -> new SchemaTableName(procedure.getSchema(), procedure.getName()));
    }

    public Procedure getProcedure(SchemaTableName name)
    {
        Procedure procedure = procedures.get(name);
        if (procedure == null) {
            throw new TrinoException(PROCEDURE_NOT_FOUND, "Procedure not registered: " + name);
        }
        return procedure;
    }

    private static void validateProcedure(Procedure procedure)
    {
        List<Class<?>> parameters = procedure.getMethodHandle().type().parameterList().stream()
                .filter(type -> !ConnectorSession.class.equals(type))
                .filter(type -> !ConnectorAccessControl.class.equals(type))
                .collect(toList());

        for (int i = 0; i < procedure.getArguments().size(); i++) {
            Argument argument = procedure.getArguments().get(i);
            Type type = argument.getType();

            Class<?> argumentType = Primitives.unwrap(parameters.get(i));
            Class<?> expectedType = getObjectType(type);
            checkArgument(
                    expectedType.equals(argumentType),
                    "Argument '%s' has invalid type %s (expected %s)",
                    argument.getName(),
                    argumentType.getName(),
                    expectedType.getName());
        }
    }

    private static Class<?> getObjectType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return boolean.class;
        }
        if (type.equals(BIGINT)) {
            return long.class;
        }
        if (type.equals(DOUBLE)) {
            return double.class;
        }
        if (type.equals(VARCHAR)) {
            return String.class;
        }
        if (type instanceof ArrayType) {
            getObjectType(type.getTypeParameters().get(0));
            return List.class;
        }
        if (type instanceof MapType) {
            getObjectType(type.getTypeParameters().get(0));
            getObjectType(type.getTypeParameters().get(1));
            return Map.class;
        }
        throw new IllegalArgumentException("Unsupported argument type: " + type.getDisplayName());
    }
}
