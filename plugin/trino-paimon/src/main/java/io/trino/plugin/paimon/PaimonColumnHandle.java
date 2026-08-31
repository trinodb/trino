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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.PaimonTypeUtils.containsVariant;
import static io.trino.spi.type.StandardTypes.JSON;
import static java.util.Objects.requireNonNull;

public final class PaimonColumnHandle
        implements ColumnHandle
{
    public static final String TRINO_ROW_ID_NAME = "$row_id";
    public static final String PAIMON_ROW_ID_NAME = "_ROW_ID";
    public static final String PAIMON_SEQUENCE_NUMBER_NAME = "_SEQUENCE_NUMBER";
    private static final String PAIMON_VALUE_KIND_NAME = "_VALUE_KIND";
    private static final String PAIMON_LEVEL_NAME = "_LEVEL";
    private static final String PAIMON_ROW_KIND_NAME = "rowkind";
    private static final String PAIMON_KEY_FIELD_PREFIX = "_KEY_";
    private final String columnName;
    private final String typeString;
    private final DataType logicalType;
    private final Type trinoType;
    private final boolean isRowId;
    private final boolean hidden;

    @JsonCreator
    public PaimonColumnHandle(
            @JsonProperty(value = "columnName", required = true) String columnName,
            @JsonProperty(value = "typeString", required = true) String typeString,
            @JsonProperty("trinoType") Type trinoType)
    {
        this(columnName, typeString, trinoType, true);
    }

    private PaimonColumnHandle(String columnName, String typeString, Type trinoType, boolean serialized)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        checkArgument(!this.columnName.isBlank(), "columnName is blank");
        this.typeString = requireNonNull(typeString, "typeString is null");
        checkArgument(!this.typeString.isBlank(), "typeString is blank");
        this.logicalType = JsonSerdeUtil.fromJson(this.typeString, DataType.class);
        if (containsVariant(logicalType)) {
            this.trinoType = requireNonNull(trinoType, "trinoType is required for Paimon VARIANT");
            checkArgument(matchesTrinoType(logicalType, this.trinoType),
                    "trinoType must match Paimon type mapping");
        }
        else {
            if (serialized) {
                checkArgument(trinoType == null, "trinoType must not be serialized for non-VARIANT columns");
            }
            else {
                checkArgument(matchesTrinoType(logicalType, requireNonNull(trinoType, "trinoType is null")),
                        "trinoType must match Paimon type mapping for non-VARIANT columns");
            }
            this.trinoType = PaimonTypeUtils.fromPaimonType(logicalType);
        }
        this.isRowId = TRINO_ROW_ID_NAME.equals(columnName);
        this.hidden = isHiddenColumnName(columnName);
    }

    public static PaimonColumnHandle of(String columnName, DataType columnType)
    {
        requireNonNull(columnType, "columnType is null");
        checkArgument(!containsVariant(columnType), "Paimon VARIANT requires TypeManager for Trino JSON type");
        return of(columnName, columnType, PaimonTypeUtils.fromPaimonType(columnType));
    }

    public static PaimonColumnHandle of(String columnName, DataType columnType, TypeManager typeManager)
    {
        requireNonNull(columnType, "columnType is null");
        requireNonNull(typeManager, "typeManager is null");
        if (!containsVariant(columnType)) {
            return of(columnName, columnType);
        }
        return of(columnName, columnType, PaimonTypeUtils.fromPaimonType(columnType, typeManager));
    }

    public static PaimonColumnHandle of(String columnName, DataType columnType, Type trinoType)
    {
        return new PaimonColumnHandle(
                columnName,
                JsonSerdeUtil.toJson(requireNonNull(columnType, "columnType is null")),
                trinoType,
                false);
    }

    @JsonAnySetter
    public void rejectUnknownJsonField(String name, Object value)
    {
        PaimonHandleJsonUtils.rejectUnknownHandleJsonField("PaimonColumnHandle", name, value);
    }

    static boolean matchesTrinoType(DataType logicalType, Type trinoType)
    {
        if (!containsVariant(logicalType)) {
            return matchesNonVariantTrinoType(logicalType, trinoType);
        }
        if (logicalType.getTypeRoot() == DataTypeRoot.VARIANT) {
            return trinoType.getBaseName().equals(JSON);
        }
        List<DataType> nestedTypes = DataTypeChecks.getNestedTypes(logicalType);
        return switch (logicalType.getTypeRoot()) {
            case ARRAY, VECTOR -> trinoType instanceof ArrayType arrayType
                    && matchesTrinoType(nestedTypes.get(0), arrayType.getElementType());
            case MULTISET -> trinoType instanceof MapType mapType
                    && matchesTrinoType(nestedTypes.get(0), mapType.getKeyType())
                    && mapType.getValueType().equals(PaimonTypeUtils.fromPaimonType(new IntType()));
            case MAP -> trinoType instanceof MapType mapType
                    && matchesTrinoType(nestedTypes.get(0), mapType.getKeyType())
                    && matchesTrinoType(nestedTypes.get(1), mapType.getValueType());
            case ROW -> trinoType instanceof RowType rowType
                    && matchesRowTypes((org.apache.paimon.types.RowType) logicalType, rowType);
            default -> throw new IllegalArgumentException("Unsupported Paimon type containing VARIANT: " + logicalType);
        };
    }

    static boolean matchesNonVariantTrinoType(DataType logicalType, Type trinoType)
    {
        return switch (logicalType.getTypeRoot()) {
            case ARRAY, VECTOR -> trinoType instanceof ArrayType arrayType
                    && matchesTrinoType(DataTypeChecks.getNestedTypes(logicalType).get(0), arrayType.getElementType());
            case MULTISET -> trinoType instanceof MapType mapType
                    && matchesTrinoType(DataTypeChecks.getNestedTypes(logicalType).get(0), mapType.getKeyType())
                    && mapType.getValueType().equals(PaimonTypeUtils.fromPaimonType(new IntType()));
            case MAP -> trinoType instanceof MapType mapType
                    && matchesTrinoType(DataTypeChecks.getNestedTypes(logicalType).get(0), mapType.getKeyType())
                    && matchesTrinoType(DataTypeChecks.getNestedTypes(logicalType).get(1), mapType.getValueType());
            case ROW -> trinoType instanceof RowType rowType
                    && matchesRowTypes((org.apache.paimon.types.RowType) logicalType, rowType);
            default -> PaimonTypeUtils.fromPaimonType(logicalType).equals(trinoType);
        };
    }

    static boolean matchesRowTypes(org.apache.paimon.types.RowType logicalType, RowType trinoType)
    {
        List<DataField> logicalFields = logicalType.getFields();
        if (logicalFields.size() != trinoType.getFields().size()) {
            return false;
        }
        for (int index = 0; index < logicalFields.size(); index++) {
            DataField logicalField = logicalFields.get(index);
            if (!matchesRowFieldName(logicalField.name(), trinoType.getFields().get(index), index)
                    || !matchesTrinoType(logicalField.type(), trinoType.getFields().get(index).getType())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesRowFieldName(String logicalName, RowType.Field trinoField, int index)
    {
        return trinoField.getName()
                .map(logicalName::equals)
                .orElseGet(() -> logicalName.equals("f" + index));
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getTypeString()
    {
        return typeString;
    }

    @JsonIgnore
    public Type getTrinoType()
    {
        return trinoType;
    }

    @JsonProperty("trinoType")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Type getSerializedTrinoType()
    {
        return containsVariant(logicalType()) ? trinoType : null;
    }

    @JsonIgnore
    public boolean isRowId()
    {
        return isRowId;
    }

    @JsonIgnore
    public boolean isHidden()
    {
        return hidden;
    }

    public DataType logicalType()
    {
        return logicalType;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(trinoType)
                .setNullable(logicalType.isNullable())
                .setHidden(hidden)
                .build();
    }

    static boolean isHiddenColumnName(String columnName)
    {
        requireNonNull(columnName, "columnName is null");
        return TRINO_ROW_ID_NAME.equals(columnName)
                || PAIMON_ROW_ID_NAME.equalsIgnoreCase(columnName)
                || PAIMON_SEQUENCE_NUMBER_NAME.equalsIgnoreCase(columnName);
    }

    static boolean isPaimonSystemColumnName(String columnName)
    {
        requireNonNull(columnName, "columnName is null");
        return TRINO_ROW_ID_NAME.equals(columnName)
                || columnName.regionMatches(true, 0, PAIMON_KEY_FIELD_PREFIX, 0, PAIMON_KEY_FIELD_PREFIX.length())
                || PAIMON_ROW_ID_NAME.equalsIgnoreCase(columnName)
                || PAIMON_SEQUENCE_NUMBER_NAME.equalsIgnoreCase(columnName)
                || PAIMON_VALUE_KIND_NAME.equalsIgnoreCase(columnName)
                || PAIMON_LEVEL_NAME.equalsIgnoreCase(columnName)
                || PAIMON_ROW_KIND_NAME.equalsIgnoreCase(columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, typeString);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PaimonColumnHandle other = (PaimonColumnHandle) obj;
        return columnName.equals(other.columnName) && typeString.equals(other.typeString);
    }

    @Override
    public String toString()
    {
        return "{" + "columnName='" + columnName + '\'' + ", typeString='" + typeString + '\'' + ", trinoType="
                + trinoType + '}';
    }
}
