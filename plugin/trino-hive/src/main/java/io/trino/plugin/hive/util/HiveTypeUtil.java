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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.HiveType;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.type.ListTypeInfo;
import io.trino.metastore.type.MapTypeInfo;
import io.trino.metastore.type.PrimitiveCategory;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.StructTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.metastore.type.UnionTypeInfo;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.lenientFormat;
import static io.trino.hive.formats.UnionToRowCoercionUtils.UNION_FIELD_FIELD_PREFIX;
import static io.trino.hive.formats.UnionToRowCoercionUtils.UNION_FIELD_TAG_NAME;
import static io.trino.hive.formats.UnionToRowCoercionUtils.UNION_FIELD_TAG_TYPE;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toTypeSignature;

public final class HiveTypeUtil
{
    private HiveTypeUtil() {}

    /**
     * @deprecated Prefer {@link #getTypeSignature(HiveType, HiveTimestampPrecision)}.
     */
    @Deprecated
    public static TypeSignature getTypeSignature(HiveType type)
    {
        return getTypeSignature(type, DEFAULT_PRECISION);
    }

    public static TypeSignature getTypeSignature(HiveType type, HiveTimestampPrecision timestampPrecision)
    {
        return toTypeSignature(type.getTypeInfo(), timestampPrecision);
    }

    public static Type getType(HiveType type, TypeManager typeManager, HiveTimestampPrecision timestampPrecision)
    {
        return typeManager.getType(getTypeSignature(type, timestampPrecision));
    }

    public static boolean typeSupported(TypeInfo typeInfo, StorageFormat storageFormat)
    {
        return switch (typeInfo.getCategory()) {
            case PRIMITIVE -> typeSupported(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
            case MAP -> typeSupported(((MapTypeInfo) typeInfo).getMapKeyTypeInfo(), storageFormat) &&
                    typeSupported(((MapTypeInfo) typeInfo).getMapValueTypeInfo(), storageFormat);
            case LIST -> typeSupported(((ListTypeInfo) typeInfo).getListElementTypeInfo(), storageFormat);
            case STRUCT -> ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos().stream().allMatch(fieldTypeInfo -> typeSupported(fieldTypeInfo, storageFormat));
            case UNION ->
                    // This feature (reading union types as structs) has only been verified against Avro and ORC tables. Here's a discussion:
                    //   1. Avro tables are supported and verified.
                    //   2. ORC tables are supported and verified.
                    //   3. The Parquet format doesn't support union types itself so there's no need to add support for it in Trino.
                    //   4. TODO: RCFile tables are not supported yet.
                    //   5. TODO: The support for Avro is done in SerDeUtils so it's possible that formats other than Avro are also supported. But verification is needed.
                    storageFormat.getSerde().equalsIgnoreCase(AVRO.getSerde()) ||
                            storageFormat.getSerde().equalsIgnoreCase(ORC.getSerde()) ||
                            ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos().stream().allMatch(fieldTypeInfo -> typeSupported(fieldTypeInfo, storageFormat));
        };
    }

    private static boolean typeSupported(PrimitiveCategory category)
    {
        return switch (category) {
            case BOOLEAN,
                    BYTE,
                    SHORT,
                    INT,
                    LONG,
                    FLOAT,
                    DOUBLE,
                    STRING,
                    VARCHAR,
                    CHAR,
                    DATE,
                    TIMESTAMP,
                    TIMESTAMPLOCALTZ,
                    BINARY,
                    DECIMAL -> true;
            case INTERVAL_YEAR_MONTH,
                    INTERVAL_DAY_TIME,
                    VOID,
                    VARIANT,
                    UNKNOWN -> false;
        };
    }

    public static Optional<HiveType> getHiveTypeForDereferences(HiveType hiveType, List<Integer> dereferences)
    {
        TypeInfo typeInfo = hiveType.getTypeInfo();
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
                        // union's tag field, defined in {@link io.trino.hive.formats.UnionToRowCoercionUtils}
                        return Optional.of(toHiveType(UNION_FIELD_TAG_TYPE));
                    }
                    typeInfo = unionTypeInfo.getAllUnionObjectTypeInfos().get(fieldIndex - 1);
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
        return Optional.of(HiveType.fromTypeInfo(typeInfo));
    }

    public static List<String> getHiveDereferenceNames(HiveType hiveType, List<Integer> dereferences)
    {
        ImmutableList.Builder<String> dereferenceNames = ImmutableList.builder();
        TypeInfo typeInfo = hiveType.getTypeInfo();
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
                typeInfo = unionTypeInfo.getAllUnionObjectTypeInfos().get(fieldIndex - 1);
                dereferenceNames.add(UNION_FIELD_FIELD_PREFIX + (fieldIndex - 1));
            }
            else {
                throw new IllegalArgumentException(lenientFormat("typeInfo: %s should be struct or union type", typeInfo));
            }
        }

        return dereferenceNames.build();
    }
}
