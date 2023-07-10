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
import org.apache.parquet.column.ColumnDescriptor;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PrimitiveField
        extends Field
{
    private final ColumnDescriptor descriptor;
    private final int id;

    public PrimitiveField(Type type, boolean required, ColumnDescriptor descriptor, int id)
    {
        super(type, descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel(), required);
        this.descriptor = requireNonNull(descriptor, "descriptor is required");
        this.id = id;
    }

    public ColumnDescriptor getDescriptor()
    {
        return descriptor;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("id", id)
                .add("repetitionLevel", getRepetitionLevel())
                .add("definitionLevel", getDefinitionLevel())
                .add("required", isRequired())
                .add("descriptor", descriptor)
                .toString();
    }
}
