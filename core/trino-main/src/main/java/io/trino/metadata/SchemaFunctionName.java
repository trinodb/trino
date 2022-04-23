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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SchemaFunctionName
{
    private final String schemaName;
    private final String functionName;

    public SchemaFunctionName(String schemaName, String functionName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        checkArgument(!schemaName.isEmpty(), "schemaName is empty");
        this.functionName = requireNonNull(functionName, "functionName is null");
        checkArgument(!functionName.isEmpty(), "functionName is empty");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, functionName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SchemaFunctionName other = (SchemaFunctionName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.functionName, other.functionName);
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + functionName;
    }
}
