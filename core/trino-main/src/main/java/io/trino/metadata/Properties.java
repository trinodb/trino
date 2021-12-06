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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class Properties
{
    private final Map<String, Object> nonNullProperties;    // properties whose values are non-null
    private final Set<String> nullPropertyNames;            // names of properties whose values are null

    Properties(Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
        requireNonNull(nonNullProperties, "nonNullProperties is null");
        requireNonNull(nullPropertyNames, "nullPropertyNames is null");
        this.nonNullProperties = ImmutableMap.copyOf(nonNullProperties);
        this.nullPropertyNames = ImmutableSet.copyOf(nullPropertyNames);
    }

    public Map<String, Object> getNonNullProperties()
    {
        return nonNullProperties;
    }

    public Set<String> getNullPropertyNames()
    {
        return nullPropertyNames;
    }
}
