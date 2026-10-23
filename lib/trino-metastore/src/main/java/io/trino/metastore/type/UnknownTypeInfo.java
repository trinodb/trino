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
package io.trino.metastore.type;

import java.util.Objects;

// based on org.apache.hadoop.hive.serde2.typeinfo.UnknownTypeInfo
public final class UnknownTypeInfo
        extends TypeInfo
{
    private static final UnknownTypeInfo INSTANCE = new UnknownTypeInfo();

    private UnknownTypeInfo() {}

    public static UnknownTypeInfo get()
    {
        return INSTANCE;
    }

    @Override
    public String getTypeName()
    {
        return "unknown";
    }

    @Override
    public Category getCategory()
    {
        return Category.UNKNOWN;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof UnknownTypeInfo;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(UnknownTypeInfo.class, getTypeName());
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return 0;
    }
}
