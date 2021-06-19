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
package io.trino.plugin.hive.orc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.HiveType;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveType.HIVE_BINARY;
import static io.trino.plugin.hive.HiveType.HIVE_BOOLEAN;
import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_DATE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;

public final class OrcTypeToHiveTypeTranslator
{
    private OrcTypeToHiveTypeTranslator() {}

    public static HiveType fromOrcTypeToHiveType(OrcType orcType, ColumnMetadata<OrcType> columnMetadata)
    {
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return HIVE_BOOLEAN;

            case FLOAT:
                return HIVE_FLOAT;

            case DOUBLE:
                return HIVE_DOUBLE;

            case BYTE:
                return HIVE_BYTE;

            case DATE:
                return HIVE_DATE;

            case SHORT:
                return HIVE_SHORT;

            case INT:
                return HIVE_INT;

            case LONG:
                return HIVE_LONG;

            case DECIMAL:
                checkArgument(orcType.getPrecision().isPresent(), "orcType.getPrecision() is not present");
                checkArgument(orcType.getScale().isPresent(), "orcType.getScale() is not present");
                return toHiveType(new DecimalTypeInfo(orcType.getPrecision().get(), orcType.getScale().get()));

            case TIMESTAMP:
                return HIVE_TIMESTAMP;

            case BINARY:
                return HIVE_BINARY;

            case CHAR:
            case VARCHAR:
            case STRING:
                return HIVE_STRING;

            case LIST: {
                HiveType elementType = fromOrcTypeToHiveType(columnMetadata.get(orcType.getFieldTypeIndex(0)), columnMetadata);
                return toHiveType(getListTypeInfo(elementType.getTypeInfo()));
            }

            case MAP: {
                HiveType keyType = getHiveType(orcType, 0, columnMetadata);
                HiveType elementType = getHiveType(orcType, 1, columnMetadata);
                return toHiveType(getMapTypeInfo(keyType.getTypeInfo(), elementType.getTypeInfo()));
            }

            case STRUCT: {
                ImmutableList.Builder<TypeInfo> fieldTypeInfo = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    fieldTypeInfo.add(getHiveType(orcType, fieldId, columnMetadata).getTypeInfo());
                }
                return toHiveType(getStructTypeInfo(orcType.getFieldNames(), fieldTypeInfo.build()));
            }

            case TIMESTAMP_INSTANT:
            case UNION:
                // unsupported
                break;
        }
        throw new VerifyException("Unhandled ORC type: " + orcType.getOrcTypeKind());
    }

    private static HiveType getHiveType(OrcType orcType, int index, ColumnMetadata<OrcType> columnMetadata)
    {
        return fromOrcTypeToHiveType(columnMetadata.get(orcType.getFieldTypeIndex(index)), columnMetadata);
    }
}
