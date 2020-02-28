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
package io.prestosql.plugin.salesforce.driver.delegates;

import java.util.Objects;

public class ForceResultField
{
    public static final String NESTED_RESULT_SET_FIELD_TYPE = "nestedResultSet";

    private String entityType;
    private String name;
    private Object value;
    private String fieldType;

    public ForceResultField(String entityType, String fieldType, String name, Object value)
    {
        super();
        this.entityType = entityType;
        this.name = name;
        this.value = value;
        this.fieldType = fieldType;
    }

    public String getEntityType()
    {
        return entityType;
    }

    public String getName()
    {
        return name;
    }

    public Object getValue()
    {
        return value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    public String getFullName()
    {
        return entityType != null ? entityType + "." + name : name;
    }

    @Override
    public String toString()
    {
        return "SfResultField [entityType=" + entityType + ", name=" + name + ", value=" + value + "]";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(entityType, name, value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj.getClass() != ForceResultField.class) {
            return false;
        }

        ForceResultField other = (ForceResultField) obj;
        return Objects.equals(entityType, other. entityType) &&
                Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    public String getFieldType()
    {
        return fieldType;
    }
}
