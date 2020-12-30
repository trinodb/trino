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
package io.prestosql.plugin.salesforce.driver.statement;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FieldDef
{
    private final String name;
    private final String type;

    public FieldDef(String name, String type)
    {
        requireNonNull(name);
        requireNonNull(type);

        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj.getClass() != FieldDef.class) {
            return false;
        }

        FieldDef other = (FieldDef) obj;
        return Objects.equals(name, other.name) && Objects.equals(type, other.type);
    }
}
