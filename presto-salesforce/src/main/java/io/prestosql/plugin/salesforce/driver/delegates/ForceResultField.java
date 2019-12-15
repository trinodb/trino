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
        final int prime = 31;
        int result = 1;
        result = prime * result + ((entityType == null) ? 0 : entityType.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ForceResultField other = (ForceResultField) obj;
        if (entityType == null) {
            if (other.entityType != null) {
                return false;
            }
        }
        else if (!entityType.equals(other.entityType)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        if (value == null) {
            return other.value == null;
        }
        else {
            return value.equals(other.value);
        }
    }

    public String getFieldType()
    {
        return fieldType;
    }
}
