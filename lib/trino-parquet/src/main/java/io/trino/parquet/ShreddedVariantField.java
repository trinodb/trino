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

/**
 * A VARIANT column stored in the shredded Parquet layout (Variant shredding spec), i.e. a group with
 * {@code metadata}, an optional {@code value}, and a {@code typed_value} child. The shredded columns
 * are modeled as {@code struct}, a synthesized {@link GroupField} over a {@code RowType} mirroring the
 * Parquet layout, so the standard nested readers can materialize them; the reader then reconstructs
 * the unshredded Variant per row from that struct block.
 */
public class ShreddedVariantField
        extends Field
{
    public static final String METADATA = "metadata";
    public static final String VALUE = "value";
    public static final String TYPED_VALUE = "typed_value";

    private final GroupField struct;

    public ShreddedVariantField(Type type, int repetitionLevel, int definitionLevel, boolean required, GroupField struct)
    {
        super(type, repetitionLevel, definitionLevel, required);
        this.struct = requireNonNull(struct, "struct is null");
    }

    public GroupField getStruct()
    {
        return struct;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("repetitionLevel", getRepetitionLevel())
                .add("definitionLevel", getDefinitionLevel())
                .add("required", isRequired())
                .add("struct", struct)
                .toString();
    }
}
