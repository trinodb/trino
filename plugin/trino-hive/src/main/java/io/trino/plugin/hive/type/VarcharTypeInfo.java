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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.VARCHAR_TYPE_NAME;

// based on org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo
public final class VarcharTypeInfo
        extends BaseCharTypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(VarcharTypeInfo.class);
    public static final int MAX_VARCHAR_LENGTH = 65535;

    public VarcharTypeInfo(int length)
    {
        super(VARCHAR_TYPE_NAME, length);
        checkArgument(length >= 1 && length <= MAX_VARCHAR_LENGTH, "invalid varchar length: %s", length);
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof VarcharTypeInfo o) &&
                (getLength() == o.getLength());
    }

    @Override
    public int hashCode()
    {
        return getLength();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getDeclaredFieldsRetainedSizeInBytes();
    }
}
