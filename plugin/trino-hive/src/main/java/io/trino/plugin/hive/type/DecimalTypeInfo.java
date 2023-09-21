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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.DECIMAL_TYPE_NAME;

// based on org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo
public final class DecimalTypeInfo
        extends PrimitiveTypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(DecimalTypeInfo.class);
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SCALE = 38;

    private final int precision;
    private final int scale;

    public DecimalTypeInfo(int precision, int scale)
    {
        super(DECIMAL_TYPE_NAME);
        this.precision = precision;
        this.scale = scale;
        checkArgument(precision >= 1 && precision <= MAX_PRECISION, "invalid decimal precision: %s", precision);
        checkArgument(scale >= 0 && scale <= MAX_SCALE, "invalid decimal scale: %s", scale);
        checkArgument(scale <= precision, "decimal precision (%s) is greater than scale (%s)", precision, scale);
    }

    @Override
    public String getTypeName()
    {
        return decimalTypeName(precision, scale);
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof DecimalTypeInfo o) &&
                (precision == o.precision) && (scale == o.scale);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, scale);
    }

    public int precision()
    {
        return precision;
    }

    public int scale()
    {
        return scale;
    }

    public static String decimalTypeName(int precision, int scale)
    {
        return DECIMAL_TYPE_NAME + "(" + precision + "," + scale + ")";
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getDeclaredFieldsRetainedSizeInBytes();
    }
}
