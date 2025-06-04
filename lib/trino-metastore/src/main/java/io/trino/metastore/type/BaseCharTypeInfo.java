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

// based on org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo
public abstract sealed class BaseCharTypeInfo
        extends PrimitiveTypeInfo
        permits CharTypeInfo, VarcharTypeInfo
{
    private final int length;

    protected BaseCharTypeInfo(String typeName, int length)
    {
        super(typeName);
        this.length = length;
    }

    public int getLength()
    {
        return length;
    }

    @Override
    public final String getTypeName()
    {
        return charTypeName(typeName, length);
    }

    public static String charTypeName(String typeName, int length)
    {
        return typeName + "(" + length + ")";
    }
}
