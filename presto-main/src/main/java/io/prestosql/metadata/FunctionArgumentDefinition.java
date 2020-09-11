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
package io.prestosql.metadata;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class FunctionArgumentDefinition
{
    private final boolean nullable;

    public FunctionArgumentDefinition(boolean nullable)
    {
        this.nullable = nullable;
    }

    public boolean isNullable()
    {
        return nullable;
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
        FunctionArgumentDefinition that = (FunctionArgumentDefinition) o;
        return nullable == that.nullable;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nullable", nullable)
                .toString();
    }
}
