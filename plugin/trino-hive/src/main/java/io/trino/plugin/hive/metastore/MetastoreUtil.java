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
package io.trino.plugin.hive.metastore;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.PartitionOfflineException;
import io.trino.plugin.hive.TableOfflineException;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.trino.plugin.hive.HiveSplitManager.PRESTO_OFFLINE;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.NUM_ROWS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.Chars.padSpaces;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.ColumnType.typeToThriftType;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public final class MetastoreUtil
{
    private static final String HIVE_PARTITION_VALUE_WILDCARD = "";

    private MetastoreUtil() {}

    public static Properties getHiveSchema(Table table)
    {
        // Mimics function in Hive: MetaStoreUtils.getTableMetadata(Table)
        return getHiveSchema(
                table.getStorage(),
                table.getDataColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    public static Properties getHiveSchema(Partition partition, Table table)
    {
        // Mimics function in Hive: MetaStoreUtils.getSchema(Partition, Table)
        return getHiveSchema(
                partition.getStorage(),
                partition.getColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    private static Properties getHiveSchema(
            Storage sd,
            List<Column> dataColumns,
            List<Column> tableDataColumns,
            Map<String, String> parameters,
            String databaseName,
            String tableName,
            List<Column> partitionKeys)
    {
        // Mimics function in Hive:
        // MetaStoreUtils.getSchema(StorageDescriptor, StorageDescriptor, Map<String, String>, String, String, List<FieldSchema>)

        Properties schema = new Properties();

        schema.setProperty(FILE_INPUT_FORMAT, sd.getStorageFormat().getInputFormat());
        schema.setProperty(FILE_OUTPUT_FORMAT, sd.getStorageFormat().getOutputFormat());

        schema.setProperty(META_TABLE_NAME, databaseName + "." + tableName);
        schema.setProperty(META_TABLE_LOCATION, sd.getLocation());

        if (sd.getBucketProperty().isPresent()) {
            schema.setProperty(BUCKET_FIELD_NAME, Joiner.on(",").join(sd.getBucketProperty().get().getBucketedBy()));
            schema.setProperty(BUCKET_COUNT, Integer.toString(sd.getBucketProperty().get().getBucketCount()));
        }
        else {
            schema.setProperty(BUCKET_COUNT, "0");
        }

        for (Map.Entry<String, String> param : sd.getSerdeParameters().entrySet()) {
            schema.setProperty(param.getKey(), (param.getValue() != null) ? param.getValue() : "");
        }
        schema.setProperty(SERIALIZATION_LIB, sd.getStorageFormat().getSerde());

        StringBuilder columnNameBuilder = new StringBuilder();
        StringBuilder columnTypeBuilder = new StringBuilder();
        StringBuilder columnCommentBuilder = new StringBuilder();
        boolean first = true;
        for (Column column : tableDataColumns) {
            if (!first) {
                columnNameBuilder.append(",");
                columnTypeBuilder.append(":");
                columnCommentBuilder.append('\0');
            }
            columnNameBuilder.append(column.getName());
            columnTypeBuilder.append(column.getType());
            columnCommentBuilder.append(column.getComment().orElse(""));
            first = false;
        }
        String columnNames = columnNameBuilder.toString();
        String columnTypes = columnTypeBuilder.toString();
        schema.setProperty(META_TABLE_COLUMNS, columnNames);
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes);
        schema.setProperty("columns.comments", columnCommentBuilder.toString());

        schema.setProperty(SERIALIZATION_DDL, toThriftDdl(tableName, dataColumns));

        StringBuilder partString = new StringBuilder();
        String partStringSep = "";
        StringBuilder partTypesString = new StringBuilder();
        String partTypesStringSep = "";
        for (Column partKey : partitionKeys) {
            partString.append(partStringSep);
            partString.append(partKey.getName());
            partTypesString.append(partTypesStringSep);
            partTypesString.append(partKey.getType().getHiveTypeName().toString());
            if (partStringSep.length() == 0) {
                partStringSep = "/";
                partTypesStringSep = ":";
            }
        }
        if (partString.length() > 0) {
            schema.setProperty(META_TABLE_PARTITION_COLUMNS, partString.toString());
            schema.setProperty(META_TABLE_PARTITION_COLUMN_TYPES, partTypesString.toString());
        }

        if (parameters != null) {
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                // add non-null parameters to the schema
                if (entry.getValue() != null) {
                    schema.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }

        return schema;
    }

    public static ProtectMode getProtectMode(Partition partition)
    {
        return getProtectMode(partition.getParameters());
    }

    public static ProtectMode getProtectMode(Table table)
    {
        return getProtectMode(table.getParameters());
    }

    public static boolean isAvroTableWithSchemaSet(Table table)
    {
        return AVRO.getSerde().equals(table.getStorage().getStorageFormat().getSerDeNullable()) &&
                (table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null ||
                        (table.getStorage().getSerdeParameters().get(AVRO_SCHEMA_URL_KEY) != null));
    }

    public static String makePartitionName(Table table, Partition partition)
    {
        return makePartitionName(table.getPartitionColumns(), partition.getValues());
    }

    public static String makePartitionName(List<Column> partitionColumns, List<String> values)
    {
        return toPartitionName(partitionColumns.stream().map(Column::getName).collect(toList()), values);
    }

    public static String toPartitionName(List<String> names, List<String> values)
    {
        checkArgument(names.size() == values.size(), "partition value count must match partition column count");
        checkArgument(values.stream().allMatch(Objects::nonNull), "partition value must not be null");

        return FileUtils.makePartName(names, values);
    }

    public static String getPartitionLocation(Table table, Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            return table.getStorage().getLocation();
        }
        return partition.get().getStorage().getLocation();
    }

    private static String toThriftDdl(String structName, List<Column> columns)
    {
        // Mimics function in Hive:
        // MetaStoreUtils.getDDLFromFieldSchema(String, List<FieldSchema>)
        StringBuilder ddl = new StringBuilder();
        ddl.append("struct ");
        ddl.append(structName);
        ddl.append(" { ");
        boolean first = true;
        for (Column column : columns) {
            if (first) {
                first = false;
            }
            else {
                ddl.append(", ");
            }
            ddl.append(typeToThriftType(column.getType().getHiveTypeName().toString()));
            ddl.append(' ');
            ddl.append(column.getName());
        }
        ddl.append("}");
        return ddl.toString();
    }

    private static ProtectMode getProtectMode(Map<String, String> parameters)
    {
        if (!parameters.containsKey(ProtectMode.PARAMETER_NAME)) {
            return new ProtectMode();
        }
        else {
            return getProtectModeFromString(parameters.get(ProtectMode.PARAMETER_NAME));
        }
    }

    public static void verifyOnline(SchemaTableName tableName, Optional<String> partitionName, ProtectMode protectMode, Map<String, String> parameters)
    {
        if (protectMode.offline) {
            if (partitionName.isPresent()) {
                throw new PartitionOfflineException(tableName, partitionName.get(), false, null);
            }
            throw new TableOfflineException(tableName, false, null);
        }

        String prestoOffline = parameters.get(PRESTO_OFFLINE);
        if (!isNullOrEmpty(prestoOffline)) {
            if (partitionName.isPresent()) {
                throw new PartitionOfflineException(tableName, partitionName.get(), true, prestoOffline);
            }
            throw new TableOfflineException(tableName, true, prestoOffline);
        }
    }

    public static void verifyCanDropColumn(HiveMetastore metastore, HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        Table table = metastore.getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        if (table.getPartitionColumns().stream().anyMatch(column -> column.getName().equals(columnName))) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition columns");
        }
        if (table.getDataColumns().size() <= 1) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop the only non-partition column in a table");
        }
    }

    public static PrincipalPrivileges buildInitialPrivilegeSet(String tableOwner)
    {
        HivePrincipal owner = new HivePrincipal(USER, tableOwner);
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.INSERT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.UPDATE, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.DELETE, true, owner, owner))
                        .build(),
                ImmutableMultimap.of());
    }

    /**
     * @param assumeCanonicalPartitionKeys allow conversion of non-char types (eg BIGINT, timestamp) to canonical string formats. If false, non-char types will be replaced
     * with the wildcard
     * @return the domain for each partition key to either the wildcard or an equals check, or empty if {@code TupleDomain.isNone()}
     */
    public static Optional<List<String>> partitionKeyFilterToStringList(List<String> columnNames, TupleDomain<String> partitionKeysFilter, boolean assumeCanonicalPartitionKeys)
    {
        if (partitionKeysFilter.isNone()) {
            return Optional.empty();
        }

        Map<String, Domain> domainMap = partitionKeysFilter.getDomains().orElseThrow(VerifyException::new);

        return Optional.of(columnNames.stream()
                .map(cn -> domainToString(domainMap.get(cn), assumeCanonicalPartitionKeys, HIVE_PARTITION_VALUE_WILDCARD))
                .collect(toImmutableList()));
    }

    /**
     * @param domain - domain expression for the column. null => TupleDomain.all()
     * @param assumeCanonicalPartitionKeys
     * @param partitionWildcardString wildcard
     * @return string for scalar values
     */
    private static String domainToString(Domain domain, boolean assumeCanonicalPartitionKeys, String partitionWildcardString)
    {
        if (domain != null && domain.isNullableSingleValue()) {
            return sqlScalarToStringForParts(domain.getType(), domain.getNullableSingleValue(), assumeCanonicalPartitionKeys, partitionWildcardString);
        }

        return partitionWildcardString;
    }

    public static boolean canConvertSqlTypeToStringForParts(Type type, boolean assumeCanonicalPartitionKeys)
    {
        return !(type instanceof TimestampType) && (type instanceof CharType || type instanceof VarcharType || assumeCanonicalPartitionKeys);
    }

    /**
     * @return canonical string representation of a given value according to its type. If there isn't a valid conversion, returns ""
     */
    public static String sqlScalarToStringForParts(Type type, Object value, boolean assumeCanonicalPartitionKeys, String partitionWildcardString)
    {
        if (!canConvertSqlTypeToStringForParts(type, assumeCanonicalPartitionKeys)) {
            return partitionWildcardString;
        }

        return sqlScalarToString(type, value, HIVE_PARTITION_VALUE_WILDCARD);
    }

    /**
     * @return canonical string representation of a given value according to its type.
     * @throws TrinoException if the type is not supported
     */
    public static String sqlScalarToString(Type type, Object value, String nullString)
    {
        if (value == null) {
            return nullString;
        }
        else if (type instanceof CharType) {
            Slice slice = (Slice) value;
            return padSpaces(slice, (CharType) type).toStringUtf8();
        }
        else if (type instanceof VarcharType) {
            Slice slice = (Slice) value;
            return slice.toStringUtf8();
        }
        else if (type instanceof DecimalType && !((DecimalType) type).isShort()) {
            return Decimals.toString((Int128) value, ((DecimalType) type).getScale());
        }
        else if (type instanceof DecimalType && ((DecimalType) type).isShort()) {
            return Decimals.toString((long) value, ((DecimalType) type).getScale());
        }
        else if (type instanceof DateType) {
            DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.date().withZoneUTC();
            return dateTimeFormatter.print(TimeUnit.DAYS.toMillis((long) value));
        }
        else if (type instanceof TimestampType) {
            // we throw on this type as we don't have timezone. Callers should not ask for this conversion type, but document for possible future work (?)
            throw new TrinoException(NOT_SUPPORTED, "TimestampType conversion to scalar expressions is not supported");
        }
        else if (type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType
                || type instanceof DoubleType
                || type instanceof RealType
                || type instanceof BooleanType) {
            return value.toString();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported partition key type: %s", type.getDisplayName()));
        }
    }

    /**
     * This method creates a TupleDomain for each partitionKey specified
     *
     * @return filtered version of relevant Domains in effectivePredicate.
     */
    public static TupleDomain<String> computePartitionKeyFilter(List<HiveColumnHandle> partitionKeys, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        checkArgument(effectivePredicate.getDomains().isPresent());

        Map<String, Domain> domains = new LinkedHashMap<>();
        for (HiveColumnHandle partitionKey : partitionKeys) {
            String name = partitionKey.getName();
            Domain domain = effectivePredicate.getDomains().get().get(partitionKey);
            if (domain != null) {
                domains.put(name, domain);
            }
        }

        return withColumnDomains(domains);
    }

    public static Map<String, String> adjustRowCount(Map<String, String> parameters, String description, long rowCountAdjustment)
    {
        String existingRowCount = parameters.get(NUM_ROWS);
        if (existingRowCount == null) {
            return parameters;
        }
        Long count = Longs.tryParse(existingRowCount);
        requireNonNull(count, format("For %s, the existing row count (%s) is not a digit string", description, existingRowCount));
        long newRowCount = count + rowCountAdjustment;
        checkArgument(newRowCount >= 0, "For %s, the subtracted row count (%s) is less than zero, existing count %s, rows deleted %s", description, newRowCount, existingRowCount, rowCountAdjustment);
        Map<String, String> copiedParameters = new HashMap<>(parameters);
        copiedParameters.put(NUM_ROWS, String.valueOf(newRowCount));
        return ImmutableMap.copyOf(copiedParameters);
    }
}
