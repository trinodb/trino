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
package io.trino.plugin.hudi.query.index;

import io.trino.spi.connector.ColumnHandle;

/**
 * Test implementation of ColumnHandle for unit tests
 */
public class TestingColumnHandle
        implements ColumnHandle
{
    private final String name;

    public TestingColumnHandle(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
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
        TestingColumnHandle other = (TestingColumnHandle) obj;
        return name.equals(other.name);
    }

    @Override
    public String toString()
    {
        return "Column:" + name;
    }
}
