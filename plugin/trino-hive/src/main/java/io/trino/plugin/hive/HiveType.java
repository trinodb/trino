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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.UnionTypeInfo;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.lenientFormat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfosFromTypeString;
import static io.trino.plugin.hive.util.HiveTypeTranslator.UNION_FIELD_FIELD_PREFIX;
import static io.trino.plugin.hive.util.HiveTypeTranslator.UNION_FIELD_TAG_NAME;
import static io.trino.plugin.hive.util.HiveTypeTranslator.UNION_FIELD_TAG_TYPE;
import static io.trino.plugin.hive.util.HiveTypeTranslator.fromPrimitiveType;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toTypeInfo;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toTypeSignature;
import static io.trino.plugin.hive.util.SerdeConstants.BIGINT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.BINARY_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.BOOLEAN_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.DATE_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.DOUBLE_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.FLOAT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.INT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.SMALLINT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.STRING_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.TIMESTAMP_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.TINYINT_TYPE_NAME;
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
    public static final HiveType HIVE_DATE = new HiveType(getPrimitiveTypeInfo(DATE_TYPE_NAME));
    public static final HiveType HIVE_BINARY = new HiveType(getPrimitiveTypeInfo(BINARY_TYPE_NAME));

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

    /**
     * @deprecated Prefer {@link #getTypeSignature(HiveTimestampPrecision)}.
     */
    @Deprecated
    public TypeSignature getTypeSignature()
    {
        return getTypeSignature(DEFAULT_PRECISION);
    }

    public TypeSignature getTypeSignature(HiveTimestampPrecision timestampPrecision)
    {
        return toTypeSignature(typeInfo, timestampPrecision);
    }

    /**
     * @deprecated Prefer {@link #getType(TypeManager, HiveTimestampPrecision)}.
     */
    @Deprecated
    public Type getType(TypeManager typeManager)
    {
        return typeManager.getType(getTypeSignature());
    }

    public Type getType(TypeManager typeManager, HiveTimestampPrecision timestampPrecision)
    {
        return typeManager.getType(getTypeSignature(timestampPrecision));
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

    public boolean isSupportedType(StorageFormat storageFormat)
    {
        return isSupportedType(getTypeInfo(), storageFormat);
    }

    public static boolean isSupportedType(TypeInfo typeInfo, StorageFormat storageFormat)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return fromPrimitiveType((PrimitiveTypeInfo) typeInfo) != null;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return isSupportedType(mapTypeInfo.getMapKeyTypeInfo(), storageFormat) && isSupportedType(mapTypeInfo.getMapValueTypeInfo(), storageFormat);
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return isSupportedType(listTypeInfo.getListElementTypeInfo(), storageFormat);
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                return structTypeInfo.getAllStructFieldTypeInfos().stream()
                        .allMatch(fieldTypeInfo -> isSupportedType(fieldTypeInfo, storageFormat));
            case UNION:
                // This feature (reading uniontypes as structs) has only been verified against Avro and ORC tables. Here's a discussion:
                //   1. Avro tables are supported and verified.
                //   2. ORC tables are supported and verified.
                //   3. The Parquet format doesn't support uniontypes itself so there's no need to add support for it in Trino.
                //   4. TODO: RCFile tables are not supported yet.
                //   5. TODO: The support for Avro is done in SerDeUtils so it's possible that formats other than Avro are also supported. But verification is needed.
                if (storageFormat.getSerde().equalsIgnoreCase(AVRO.getSerde()) || storageFormat.getSerde().equalsIgnoreCase(ORC.getSerde())) {
                    UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                    return unionTypeInfo.getAllUnionObjectTypeInfos().stream()
                            .allMatch(fieldTypeInfo -> isSupportedType(fieldTypeInfo, storageFormat));
                }
        }
        return false;
    }

    @JsonCreator
    public static HiveType valueOf(String hiveTypeName)
    {
        requireNonNull(hiveTypeName, "hiveTypeName is null");
        return toHiveType(getTypeInfoFromTypeString(hiveTypeName));
    }

    public static List<HiveType> toHiveTypes(String hiveTypes)
    {
        requireNonNull(hiveTypes, "hiveTypes is null");
        return getTypeInfosFromTypeString(hiveTypes).stream()
                .map(HiveType::toHiveType)
                .collect(toImmutableList());
    }

    public static HiveType toHiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        return new HiveType(typeInfo);
    }

    public static HiveType toHiveType(Type type)
    {
        return new HiveType(toTypeInfo(type));
    }

    public Optional<HiveType> getHiveTypeForDereferences(List<Integer> dereferences)
    {
        TypeInfo typeInfo = getTypeInfo();
        for (int fieldIndex : dereferences) {
            if (typeInfo instanceof StructTypeInfo structTypeInfo) {
                try {
                    typeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
                }
                catch (RuntimeException e) {
                    // return empty when failed to dereference, this could happen when partition and table schema mismatch
                    return Optional.empty();
                }
            }
            else if (typeInfo instanceof UnionTypeInfo unionTypeInfo) {
                try {
                    if (fieldIndex == 0) {
                        //  union's tag field, defined in {@link io.trino.plugin.hive.util.HiveTypeTranslator#toTypeSignature}
                        return Optional.of(HiveType.toHiveType(UNION_FIELD_TAG_TYPE));
                    }
                    else {
                        typeInfo = unionTypeInfo.getAllUnionObjectTypeInfos().get(fieldIndex - 1);
                    }
                }
                catch (RuntimeException e) {
                    // return empty when failed to dereference, this could happen when partition and table schema mismatch
                    return Optional.empty();
                }
            }
            else {
                throw new IllegalArgumentException(lenientFormat("typeInfo: %s should be struct or union type", typeInfo));
            }
        }
        return Optional.of(toHiveType(typeInfo));
    }

    public List<String> getHiveDereferenceNames(List<Integer> dereferences)
    {
        ImmutableList.Builder<String> dereferenceNames = ImmutableList.builder();
        TypeInfo typeInfo = getTypeInfo();
        for (int i = 0; i < dereferences.size(); i++) {
            int fieldIndex = dereferences.get(i);
            checkArgument(fieldIndex >= 0, "fieldIndex cannot be negative");

            if (typeInfo instanceof StructTypeInfo structTypeInfo) {
                checkArgument(fieldIndex < structTypeInfo.getAllStructFieldNames().size(),
                        "fieldIndex should be less than the number of fields in the struct");

                String fieldName = structTypeInfo.getAllStructFieldNames().get(fieldIndex);
                dereferenceNames.add(fieldName);
                typeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
            }
            else if (typeInfo instanceof UnionTypeInfo unionTypeInfo) {
                checkArgument((fieldIndex - 1) < unionTypeInfo.getAllUnionObjectTypeInfos().size(),
                        "fieldIndex should be less than the number of fields in the union plus tag field");

                if (fieldIndex == 0) {
                    checkArgument(i == (dereferences.size() - 1), "Union's tag field should not have more subfields");
                    dereferenceNames.add(UNION_FIELD_TAG_NAME);
                    break;
                }
                else {
                    typeInfo = unionTypeInfo.getAllUnionObjectTypeInfos().get(fieldIndex - 1);
                    dereferenceNames.add(UNION_FIELD_FIELD_PREFIX + (fieldIndex - 1));
                }
            }
            else {
                throw new IllegalArgumentException(lenientFormat("typeInfo: %s should be struct or union type", typeInfo));
            }
        }

        return dereferenceNames.build();
    }

    public long getRetainedSizeInBytes()
    {
        // typeInfo is not accounted for as the instances are cached (by TypeInfoFactory) and shared
        return INSTANCE_SIZE + hiveTypeName.getEstimatedSizeInBytes();
    }
}
