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
package io.trino.plugin.bigquery.type;

import static com.google.cloud.bigquery.StandardSQLTypeName.NUMERIC;
import static com.google.common.base.Preconditions.checkArgument;

public final class DecimalTypeInfo
        extends PrimitiveTypeInfo
{
    private static final int MAX_PRECISION_MINUS_SCALE = 29;
    private static final int MAX_SCALE = 9;

    private final int precision;
    private final int scale;

    public DecimalTypeInfo(int precision, int scale)
    {
        super(NUMERIC.name());
        this.precision = precision;
        this.scale = scale;
        checkArgument(scale >= 0 && scale <= MAX_SCALE, "invalid decimal scale: %s", scale);
        checkArgument(precision >= 1 && precision <= MAX_PRECISION_MINUS_SCALE + scale, "invalid decimal precision: %s", precision);
        checkArgument(scale <= precision, "invalid decimal precision: %s is lower than scale %s", precision, scale);
    }

    @Override
    public String toString()
    {
        return decimalTypeName(precision, scale);
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
        return NUMERIC.name() + "(" + precision + ", " + scale + ")";
    }
}
