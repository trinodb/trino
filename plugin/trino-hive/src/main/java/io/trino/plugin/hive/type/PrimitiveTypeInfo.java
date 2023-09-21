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
package io.trino.plugin.hive.type;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeEntryFromTypeName;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
public sealed class PrimitiveTypeInfo
        extends TypeInfo
        permits BaseCharTypeInfo, DecimalTypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(PrimitiveTypeInfo.class);

    protected final String typeName;
    private final PrimitiveCategory primitiveCategory;

    PrimitiveTypeInfo(String typeName)
    {
        this.typeName = requireNonNull(typeName, "typeName is null");
        this.primitiveCategory = getTypeEntryFromTypeName(typeName).primitiveCategory();
    }

    @Override
    public Category getCategory()
    {
        return Category.PRIMITIVE;
    }

    public PrimitiveCategory getPrimitiveCategory()
    {
        return primitiveCategory;
    }

    @Override
    public String getTypeName()
    {
        return typeName;
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof PrimitiveTypeInfo o)
                && typeName.equals(o.typeName);
    }

    /**
     * Generate the hashCode for this TypeInfo.
     */
    @Override
    public int hashCode()
    {
        return typeName.hashCode();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        verify(getClass() == PrimitiveTypeInfo.class, "Method must be overridden in %s", getClass());
        return INSTANCE_SIZE + getDeclaredFieldsRetainedSizeInBytes();
    }

    protected long getDeclaredFieldsRetainedSizeInBytes()
    {
        return estimatedSizeOf(typeName);
    }
}
