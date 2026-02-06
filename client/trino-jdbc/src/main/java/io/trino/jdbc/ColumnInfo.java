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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import io.trino.client.ClientStandardTypes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;

import java.sql.Types;
import java.util.List;

import static java.util.Objects.requireNonNull;

class ColumnInfo
{
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();

    private final int columnType;
    private final String columnJavaClassName;
    private final List<Integer> columnParameterTypes;
    private final ClientTypeSignature columnTypeSignature;
    private final Nullable nullable;
    private final boolean currency;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int columnDisplaySize;
    private final String columnLabel;
    private final String columnName;
    private final String tableName;
    private final String schemaName;
    private final String catalogName;

    public enum Nullable
    {
        NO_NULLS, NULLABLE, UNKNOWN
    }

    public ColumnInfo(
            int columnType,
            String columnJavaClassName,
            List<Integer> columnParameterTypes,
            ClientTypeSignature columnTypeSignature,
            Nullable nullable,
            boolean currency,
            boolean signed,
            int precision,
            int scale,
            int columnDisplaySize,
            String columnLabel,
            String columnName,
            String tableName,
            String schemaName,
            String catalogName)
    {
        this.columnType = columnType;
        this.columnJavaClassName = requireNonNull(columnJavaClassName, "columnJavaClassName is null");
        this.columnParameterTypes = ImmutableList.copyOf(requireNonNull(columnParameterTypes, "columnParameterTypes is null"));
        this.columnTypeSignature = requireNonNull(columnTypeSignature, "columnTypeSignature is null");
        this.nullable = requireNonNull(nullable, "nullable is null");
        this.currency = currency;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.columnDisplaySize = columnDisplaySize;
        this.columnLabel = requireNonNull(columnLabel, "columnLabel is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    public static void setTypeInfo(Builder builder, ClientTypeSignature type)
    {
        TypeInfo typeInfo = typeInfo(type);
        builder.setColumnType(typeInfo.jdbcType);
        builder.setColumnJavaClassName(typeInfo.jdbcJavaClass.getName());
        ImmutableList.Builder<Integer> parameterTypes = ImmutableList.builder();
        if (!type.getArguments().isEmpty()) {
            for (ClientTypeSignatureParameter parameter : type.getArguments()) {
                parameterTypes.add(getType(parameter));
            }
        }
        builder.setColumnParameterTypes(parameterTypes.build());
        switch (type.getRawType()) {
            case "boolean":
                builder.setColumnDisplaySize(5);
                break;
            case "bigint":
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setScale(0);
                builder.setColumnDisplaySize(20);
                break;
            case "integer":
                builder.setSigned(true);
                builder.setPrecision(10);
                builder.setScale(0);
                builder.setColumnDisplaySize(11);
                break;
            case "smallint":
                builder.setSigned(true);
                builder.setPrecision(5);
                builder.setScale(0);
                builder.setColumnDisplaySize(6);
                break;
            case "tinyint":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(4);
                break;
            case "real":
                builder.setSigned(true);
                builder.setPrecision(9);
                builder.setScale(0);
                builder.setColumnDisplaySize(16);
                break;
            case "double":
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setScale(0);
                builder.setColumnDisplaySize(24);
                break;
            case "char":
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(type.getArguments().get(0).getLongLiteral().intValue());
                builder.setColumnDisplaySize(type.getArguments().get(0).getLongLiteral().intValue());
                break;
            case "varchar":
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(type.getArguments().get(0).getLongLiteral().intValue());
                builder.setColumnDisplaySize(type.getArguments().get(0).getLongLiteral().intValue());
                break;
            case "varbinary":
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(VARBINARY_MAX);
                builder.setColumnDisplaySize(VARBINARY_MAX);
                break;
            case "time":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIME_MAX);
                break;
            case "time with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIME_WITH_TIME_ZONE_MAX);
                break;
            case "timestamp":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case "timestamp with time zone":
                builder.setSigned(true);
                builder.setPrecision(3);
                builder.setScale(0);
                builder.setColumnDisplaySize(TIMESTAMP_WITH_TIME_ZONE_MAX);
                break;
            case "date":
                builder.setSigned(true);
                builder.setScale(0);
                builder.setColumnDisplaySize(DATE_MAX);
                break;
            case "interval year to month":
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case "interval day to second":
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case "decimal":
                builder.setSigned(true);
                builder.setColumnDisplaySize(type.getArguments().get(0).getLongLiteral().intValue() + 2); // dot and sign
                builder.setPrecision(type.getArguments().get(0).getLongLiteral().intValue());
                builder.setScale(type.getArguments().get(1).getLongLiteral().intValue());
                break;
        }
    }

    private static int getType(ClientTypeSignatureParameter typeParameter)
    {
        switch (typeParameter.getKind()) {
            case TYPE:
                return typeInfo(typeParameter.getTypeSignature()).jdbcType;
            default:
                return Types.JAVA_OBJECT;
        }
    }

    private static TypeInfo typeInfo(ClientTypeSignature type)
    {
        switch (type.getRawType()) {
            case ClientStandardTypes.BOOLEAN:
                return new TypeInfo(Types.BOOLEAN, Boolean.class);

            case ClientStandardTypes.TINYINT:
                return new TypeInfo(Types.TINYINT, Byte.class);
            case ClientStandardTypes.SMALLINT:
                return new TypeInfo(Types.SMALLINT, Short.class);
            case ClientStandardTypes.INTEGER:
                return new TypeInfo(Types.INTEGER, Integer.class);
            case ClientStandardTypes.BIGINT:
                return new TypeInfo(Types.BIGINT, Long.class);

            case ClientStandardTypes.REAL:
                return new TypeInfo(Types.REAL, Float.class);
            case ClientStandardTypes.DOUBLE:
                return new TypeInfo(Types.DOUBLE, Double.class);

            case ClientStandardTypes.DECIMAL:
                return new TypeInfo(Types.DECIMAL, java.math.BigDecimal.class);
            case ClientStandardTypes.NUMBER:
                // NUMERIC/DECIMAL type could suggest clients to use ResultSet.getBigDecimal, but this will fail for some values. OTHER feel safer.
                return new TypeInfo(Types.OTHER, java.lang.Number.class);

            case ClientStandardTypes.VARCHAR:
                return new TypeInfo(Types.VARCHAR, String.class);
            case ClientStandardTypes.CHAR:
                return new TypeInfo(Types.CHAR, String.class);
            case ClientStandardTypes.VARBINARY:
                return new TypeInfo(Types.VARBINARY, byte[].class);

            case ClientStandardTypes.DATE:
                return new TypeInfo(Types.DATE, java.sql.Date.class);
            case ClientStandardTypes.TIME:
                return new TypeInfo(Types.TIME, java.sql.Time.class);
            case ClientStandardTypes.TIME_WITH_TIME_ZONE:
                return new TypeInfo(Types.TIME_WITH_TIMEZONE, java.sql.Time.class);
            case ClientStandardTypes.TIMESTAMP:
                return new TypeInfo(Types.TIMESTAMP, java.sql.Timestamp.class);
            case ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return new TypeInfo(Types.TIMESTAMP_WITH_TIMEZONE, java.sql.Timestamp.class);

            case ClientStandardTypes.ARRAY:
                return new TypeInfo(Types.ARRAY, java.sql.Array.class);
            case ClientStandardTypes.MAP:
                return new TypeInfo(Types.JAVA_OBJECT, java.util.Map.class);
            case ClientStandardTypes.ROW:
                return new TypeInfo(Types.JAVA_OBJECT, io.trino.jdbc.Row.class);

            case ClientStandardTypes.JSON:
            case ClientStandardTypes.IPADDRESS:
            case ClientStandardTypes.UUID:
                return new TypeInfo(Types.JAVA_OBJECT, String.class);

            case ClientStandardTypes.UNKNOWN:
                return new TypeInfo(Types.NULL, Object.class);

            default:
                return new TypeInfo(Types.JAVA_OBJECT, Object.class);
        }
    }

    public int getColumnType()
    {
        return columnType;
    }

    public String getColumnJavaClassName()
    {
        return columnJavaClassName;
    }

    public List<Integer> getColumnParameterTypes()
    {
        return columnParameterTypes;
    }

    public String getColumnTypeName()
    {
        return columnTypeSignature.toString();
    }

    public ClientTypeSignature getColumnTypeSignature()
    {
        return columnTypeSignature;
    }

    public Nullable getNullable()
    {
        return nullable;
    }

    public boolean isCurrency()
    {
        return currency;
    }

    public boolean isSigned()
    {
        return signed;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public int getColumnDisplaySize()
    {
        return columnDisplaySize;
    }

    public String getColumnLabel()
    {
        return columnLabel;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    static class Builder
    {
        private int columnType;
        private String columnJavaClassName;
        private List<Integer> columnParameterTypes;
        private ClientTypeSignature columnTypeSignature;
        private Nullable nullable;
        private boolean currency;
        private boolean signed;
        private int precision;
        private int scale;
        private int columnDisplaySize;
        private String columnLabel;
        private String columnName;
        private String tableName;
        private String schemaName;
        private String catalogName;

        @CanIgnoreReturnValue
        public Builder setColumnType(int columnType)
        {
            this.columnType = columnType;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnJavaClassName(String columnJavaClassName)
        {
            this.columnJavaClassName = columnJavaClassName;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnParameterTypes(List<Integer> columnParameterTypes)
        {
            this.columnParameterTypes = ImmutableList.copyOf(requireNonNull(columnParameterTypes, "columnParameterTypes is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnTypeSignature(ClientTypeSignature columnTypeSignature)
        {
            this.columnTypeSignature = columnTypeSignature;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setNullable(Nullable nullable)
        {
            this.nullable = nullable;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setCurrency(boolean currency)
        {
            this.currency = currency;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setSigned(boolean signed)
        {
            this.signed = signed;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setPrecision(int precision)
        {
            this.precision = precision;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setScale(int scale)
        {
            this.scale = scale;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnDisplaySize(int columnDisplaySize)
        {
            this.columnDisplaySize = columnDisplaySize;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnLabel(String columnLabel)
        {
            this.columnLabel = columnLabel;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setSchemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        @CheckReturnValue
        public ColumnInfo build()
        {
            return new ColumnInfo(
                    columnType,
                    columnJavaClassName,
                    columnParameterTypes,
                    columnTypeSignature,
                    nullable,
                    currency,
                    signed,
                    precision,
                    scale,
                    columnDisplaySize,
                    columnLabel,
                    columnName,
                    tableName,
                    schemaName,
                    catalogName);
        }
    }

    private static class TypeInfo
    {
        private final int jdbcType;
        private final Class<?> jdbcJavaClass;

        public TypeInfo(int jdbcType, Class<?> jdbcJavaClass)
        {
            this.jdbcType = jdbcType;
            this.jdbcJavaClass = requireNonNull(jdbcJavaClass, "jdbcJavaClass is null");
        }
    }
}
