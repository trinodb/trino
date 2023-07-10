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
package io.trino.spi.connector;

import java.util.Objects;

import static java.util.Locale.ENGLISH;

public final class SchemaRoutineName
{
    private final String schemaName;
    private final String routineName;

    public SchemaRoutineName(String schemaName, String routineName)
    {
        this.schemaName = schemaName.toLowerCase(ENGLISH);
        this.routineName = routineName.toLowerCase(ENGLISH);
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getRoutineName()
    {
        return routineName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaRoutineName that = (SchemaRoutineName) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(routineName, that.routineName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, routineName);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + routineName;
    }
}
