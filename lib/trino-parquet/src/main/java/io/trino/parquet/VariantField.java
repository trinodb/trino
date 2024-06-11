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
package io.trino.parquet;

import io.trino.spi.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class VariantField
        extends Field
{
    private final Field value;
    private final Field metadata;

    public VariantField(Type type, int repetitionLevel, int definitionLevel, boolean required, Field value, Field metadata)
    {
        super(type, repetitionLevel, definitionLevel, required);
        this.value = requireNonNull(value, "value is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Field getValue()
    {
        return value;
    }

    public Field getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("repetitionLevel", getRepetitionLevel())
                .add("definitionLevel", getDefinitionLevel())
                .add("required", isRequired())
                .add("value", value)
                .add("metadata", getMetadata())
                .toString();
    }
}
