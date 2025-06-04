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
package io.trino.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.metastore.type.Category;
import io.trino.metastore.type.TypeInfo;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.metastore.type.TypeConstants.BIGINT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.BINARY_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.BOOLEAN_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.DATE_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.DOUBLE_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.FLOAT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.INT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.SMALLINT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.STRING_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.TIMESTAMPLOCALTZ_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.TIMESTAMP_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.TINYINT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.VARIANT_TYPE_NAME;
import static io.trino.metastore.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.metastore.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static io.trino.metastore.type.TypeInfoUtils.getTypeInfosFromTypeString;
import static java.util.Objects.requireNonNull;

public final class HiveType
{
    private static final int INSTANCE_SIZE = instanceSize(HiveType.class);

    public static final HiveType HIVE_BOOLEAN = new HiveType(getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME));
    public static final HiveType HIVE_BYTE = new HiveType(getPrimitiveTypeInfo(TINYINT_TYPE_NAME));
    public static final HiveType HIVE_SHORT = new HiveType(getPrimitiveTypeInfo(SMALLINT_TYPE_NAME));
    public static final HiveType HIVE_INT = new HiveType(getPrimitiveTypeInfo(INT_TYPE_NAME));
    public static final HiveType HIVE_LONG = new HiveType(getPrimitiveTypeInfo(BIGINT_TYPE_NAME));
    public static final HiveType HIVE_FLOAT = new HiveType(getPrimitiveTypeInfo(FLOAT_TYPE_NAME));
    public static final HiveType HIVE_DOUBLE = new HiveType(getPrimitiveTypeInfo(DOUBLE_TYPE_NAME));
    public static final HiveType HIVE_STRING = new HiveType(getPrimitiveTypeInfo(STRING_TYPE_NAME));
    public static final HiveType HIVE_TIMESTAMP = new HiveType(getPrimitiveTypeInfo(TIMESTAMP_TYPE_NAME));
    public static final HiveType HIVE_TIMESTAMPLOCALTZ = new HiveType(getPrimitiveTypeInfo(TIMESTAMPLOCALTZ_TYPE_NAME));
    public static final HiveType HIVE_DATE = new HiveType(getPrimitiveTypeInfo(DATE_TYPE_NAME));
    public static final HiveType HIVE_BINARY = new HiveType(getPrimitiveTypeInfo(BINARY_TYPE_NAME));
    public static final HiveType HIVE_VARIANT = new HiveType(getPrimitiveTypeInfo(VARIANT_TYPE_NAME));

    private final HiveTypeName hiveTypeName;
    private final TypeInfo typeInfo;

    private HiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        this.hiveTypeName = new HiveTypeName(typeInfo.getTypeName());
        this.typeInfo = typeInfo;
    }

    public HiveTypeName getHiveTypeName()
    {
        return hiveTypeName;
    }

    public Category getCategory()
    {
        return typeInfo.getCategory();
    }

    public TypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveType hiveType = (HiveType) o;

        return hiveTypeName.equals(hiveType.hiveTypeName);
    }

    @Override
    public int hashCode()
    {
        return hiveTypeName.hashCode();
    }

    @JsonValue
    @Override
    public String toString()
    {
        return hiveTypeName.toString();
    }

    @JsonCreator
    public static HiveType valueOf(String hiveTypeName)
    {
        requireNonNull(hiveTypeName, "hiveTypeName is null");
        return fromTypeInfo(getTypeInfoFromTypeString(hiveTypeName));
    }

    public static List<HiveType> toHiveTypes(String hiveTypes)
    {
        requireNonNull(hiveTypes, "hiveTypes is null");
        return getTypeInfosFromTypeString(hiveTypes).stream()
                .map(HiveType::fromTypeInfo)
                .collect(toImmutableList());
    }

    public static HiveType fromTypeInfo(TypeInfo typeInfo)
    {
        return new HiveType(typeInfo);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + hiveTypeName.getEstimatedSizeInBytes() + typeInfo.getRetainedSizeInBytes();
    }
}
