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
package io.trino.plugin.hive.projection;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;

import java.util.List;
import java.util.Optional;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DateProjection.class, name = "date"),
        @JsonSubTypes.Type(value = EnumProjection.class, name = "enum"),
        @JsonSubTypes.Type(value = InjectedProjection.class, name = "injected"),
        @JsonSubTypes.Type(value = IntegerProjection.class, name = "integer")
})
public sealed interface Projection
        permits DateProjection, EnumProjection, InjectedProjection, IntegerProjection
{
    List<String> getProjectedValues(Optional<Domain> partitionValueFilter);

    default Optional<NullableValue> parsePartitionValue(String value)
    {
        return Optional.empty();
    }

    default Optional<String> toPartitionValue(Object value)
    {
        return Optional.empty();
    }
}
