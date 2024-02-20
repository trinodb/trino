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
package io.trino.plugin.thrift.api;

import com.google.common.collect.ImmutableSet;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.plugin.thrift.api.NameValidationUtils.checkValidName;

@ThriftStruct
public final class TrinoThriftTupleDomain
{
    private final Map<String, TrinoThriftDomain> domains;

    @ThriftConstructor
    public TrinoThriftTupleDomain(@Nullable Map<String, TrinoThriftDomain> domains)
    {
        if (domains != null) {
            for (String name : domains.keySet()) {
                checkValidName(name);
            }
        }
        this.domains = domains;
    }

    /**
     * Return a map of column names to constraints.
     */
    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public Map<String, TrinoThriftDomain> getDomains()
    {
        return domains;
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
        TrinoThriftTupleDomain other = (TrinoThriftTupleDomain) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domains);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnsWithConstraints", domains != null ? domains.keySet() : ImmutableSet.of())
                .toString();
    }
}
