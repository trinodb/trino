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
package io.prestosql.parquet;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GroupField
        extends Field
{
    private final List<Optional<Field>> children;

    public GroupField(Type type, int repetitionLevel, int definitionLevel, boolean required, List<Optional<Field>> children)
    {
        super(type, repetitionLevel, definitionLevel, required);
        checkArgument(
                type.getTypeParameters().size() == children.size(),
                "Type %s has %s parameters, but %s children: %s",
                type,
                type.getTypeParameters().size(),
                children.size(),
                children);
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public List<Optional<Field>> getChildren()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("repetitionLevel", getRepetitionLevel())
                .add("definitionLevel", getDefinitionLevel())
                .add("required", isRequired())
                .add("children", getChildren().stream()
                        .map(field -> field.orElse(null))
                        .collect(toList()))
                .toString();
    }
}
