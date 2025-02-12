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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Shorts;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
import io.airlift.json.JsonCodec;
import io.trino.hive.thrift.metastore.BinaryColumnStatsData;
import io.trino.hive.thrift.metastore.BooleanColumnStatsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.hive.thrift.metastore.Date;
import io.trino.hive.thrift.metastore.DateColumnStatsData;
import io.trino.hive.thrift.metastore.Decimal;
import io.trino.hive.thrift.metastore.DecimalColumnStatsData;
import io.trino.hive.thrift.metastore.DoubleColumnStatsData;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.FunctionType;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.Order;
import io.trino.hive.thrift.metastore.PrincipalPrivilegeSet;
import io.trino.hive.thrift.metastore.PrivilegeGrantInfo;
import io.trino.hive.thrift.metastore.ResourceUri;
import io.trino.hive.thrift.metastore.RolePrincipalGrant;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.StringColumnStatsData;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.io.BaseEncoding.base64Url;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.binaryStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.booleanStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.dateStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.decimalStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.doubleStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.longStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.stringStats;
import static io.trino.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static io.trino.metastore.type.Category.PRIMITIVE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static io.trino.plugin.hive.HiveColumnStatisticType.MIN_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static io.trino.plugin.hive.HiveColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_LITERAL_KEY;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toResourceUris;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ThriftMetastoreUtil
{
    private static final JsonCodec<LanguageFunction> LANGUAGE_FUNCTION_CODEC = JsonCodec.jsonCodec(LanguageFunction.class);
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String ADMIN_ROLE_NAME = "admin";

    private ThriftMetastoreUtil() {}

    public static io.trino.hive.thrift.metastore.Database toMetastoreApiDatabase(Database database)
    {
        io.trino.hive.thrift.metastore.Database result = new io.trino.hive.thrift.metastore.Database();
        result.setName(database.getDatabaseName());
        database.getLocation().ifPresent(result::setLocationUri);
        result.setOwnerName(database.getOwnerName().orElse(null));

        result.setOwnerType(database.getOwnerType().map(ThriftMetastoreUtil::fromTrinoPrincipalType).orElse(null));
        database.getComment().ifPresent(result::setDescription);
        result.setParameters(database.getParameters());
        return result;
    }

    public static io.trino.hive.thrift.metastore.Table toMetastoreApiTable(Table table, PrincipalPrivileges privileges)
    {
        io.trino.hive.thrift.metastore.Table result = toMetastoreApiTable(table);
        result.setPrivileges(toMetastoreApiPrincipalPrivilegeSet(privileges));
        return result;
    }

    public static io.trino.hive.thrift.metastore.Table toMetastoreApiTable(Table table)
    {
        io.trino.hive.thrift.metastore.Table result = new io.trino.hive.thrift.metastore.Table();
        result.setDbName(table.getDatabaseName());
        result.setTableName(table.getTableName());
        result.setOwner(table.getOwner().orElse(null));
        result.setTableType(table.getTableType());
        result.setParameters(table.getParameters());
        result.setPartitionKeys(table.getPartitionColumns().stream().map(ThriftMetastoreUtil::toMetastoreApiFieldSchema).collect(toImmutableList()));
        result.setSd(makeStorageDescriptor(table.getTableName(), table.getDataColumns(), table.getStorage()));
        result.setViewOriginalText(table.getViewOriginalText().orElse(null));
        result.setViewExpandedText(table.getViewExpandedText().orElse(null));
        table.getWriteId().ifPresent(result::setWriteId);
        return result;
    }

    private static PrincipalPrivilegeSet toMetastoreApiPrincipalPrivilegeSet(PrincipalPrivileges privileges)
    {
        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> userPrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getUserPrivileges().asMap().entrySet()) {
            userPrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                    .collect(toImmutableList()));
        }

        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> rolePrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getRolePrivileges().asMap().entrySet()) {
            rolePrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                    .collect(toImmutableList()));
        }

        return new PrincipalPrivilegeSet(userPrivileges.buildOrThrow(), ImmutableMap.of(), rolePrivileges.buildOrThrow());
    }

    public static PrivilegeGrantInfo toMetastoreApiPrivilegeGrantInfo(HivePrivilegeInfo privilegeInfo)
    {
        return new PrivilegeGrantInfo(
                privilegeInfo.getHivePrivilege().name().toLowerCase(ENGLISH),
                0,
                privilegeInfo.getGrantor().getName(),
                fromTrinoPrincipalType(privilegeInfo.getGrantor().getType()),
                privilegeInfo.isGrantOption());
    }

    public static Stream<RoleGrant> listApplicableRoles(HivePrincipal principal, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants)
    {
        return Streams.stream(new AbstractIterator<>()
        {
            private final Queue<RoleGrant> output = new ArrayDeque<>();
            private final Set<RoleGrant> seenRoles = new HashSet<>();
            private final Queue<HivePrincipal> queue = new ArrayDeque<>();

            {
                queue.add(principal);
            }

            @Override
            protected RoleGrant computeNext()
            {
                while (output.isEmpty() && !queue.isEmpty()) {
                    Set<RoleGrant> grants = listRoleGrants.apply(queue.remove());
                    for (RoleGrant grant : grants) {
                        if (seenRoles.add(grant)) {
                            output.add(grant);
                            queue.add(new HivePrincipal(ROLE, grant.getRoleName()));
                        }
                    }
                }
                if (!output.isEmpty()) {
                    return output.remove();
                }
                return endOfData();
            }
        });
    }

    public static boolean isRoleApplicable(HivePrincipal principal, String role, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants)
    {
        if (principal.getType() == ROLE && principal.getName().equals(role)) {
            return true;
        }
        return listApplicableRoleNames(principal, listRoleGrants)
                .anyMatch(role::equals);
    }

    private static Stream<String> listApplicableRoleNames(HivePrincipal principal, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants)
    {
        return listApplicableRoles(principal, listRoleGrants)
                .map(RoleGrant::getRoleName);
    }

    public static Stream<HivePrincipal> listEnabledPrincipals(ConnectorIdentity identity, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants)
    {
        return Stream.concat(
                Stream.of(new HivePrincipal(USER, identity.getUser())),
                listEnabledRoles(identity, listRoleGrants)
                        .map(role -> new HivePrincipal(ROLE, role)));
    }

    public static boolean isRoleEnabled(ConnectorIdentity identity, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants, String role)
    {
        if (role.equals(PUBLIC_ROLE_NAME)) {
            return true;
        }

        if (identity.getConnectorRole().isPresent() && identity.getConnectorRole().get().getType() == SelectedRole.Type.NONE) {
            return false;
        }

        HivePrincipal principal = HivePrincipal.from(identity);

        if (principal.getType() == ROLE && principal.getName().equals(role)) {
            return true;
        }

        if (role.equals(ADMIN_ROLE_NAME)) {
            // The admin role must be enabled explicitly, and so it should be checked above
            return false;
        }

        // all the above code could be removed and method semantic would remain the same, however it would be more expensive for some negative cases (see above)
        return listEnabledRoles(identity, listRoleGrants)
                .anyMatch(role::equals);
    }

    public static Stream<String> listEnabledRoles(ConnectorIdentity identity, Function<HivePrincipal, Set<RoleGrant>> listRoleGrants)
    {
        if (identity.getConnectorRole().isPresent() && identity.getConnectorRole().get().getType() == SelectedRole.Type.NONE) {
            return Stream.of(PUBLIC_ROLE_NAME);
        }
        HivePrincipal principal = HivePrincipal.from(identity);

        Stream<String> roles = Stream.of(PUBLIC_ROLE_NAME);

        if (principal.getType() == ROLE) {
            roles = Stream.concat(roles, Stream.of(principal.getName()));
        }

        return Stream.concat(
                roles,
                listApplicableRoles(principal, listRoleGrants)
                        .map(RoleGrant::getRoleName)
                        // The admin role must be enabled explicitly. If it is, it was added above.
                        .filter(Predicate.isEqual(ADMIN_ROLE_NAME).negate()))
                // listApplicableRoles may return role which was already added explicitly above.
                .distinct();
    }

    public static io.trino.hive.thrift.metastore.Partition toMetastoreApiPartition(PartitionWithStatistics partitionWithStatistics)
    {
        io.trino.hive.thrift.metastore.Partition partition = toMetastoreApiPartition(partitionWithStatistics.getPartition());
        partition.setParameters(updateStatisticsParameters(partition.getParameters(), partitionWithStatistics.getStatistics().basicStatistics()));
        return partition;
    }

    public static io.trino.hive.thrift.metastore.Partition toMetastoreApiPartition(Partition partition)
    {
        return toMetastoreApiPartition(partition, Optional.empty());
    }

    public static io.trino.hive.thrift.metastore.Partition toMetastoreApiPartition(Partition partition, Optional<Long> writeId)
    {
        io.trino.hive.thrift.metastore.Partition result = new io.trino.hive.thrift.metastore.Partition();
        result.setDbName(partition.getDatabaseName());
        result.setTableName(partition.getTableName());
        result.setValues(partition.getValues());
        result.setSd(makeStorageDescriptor(partition.getTableName(), partition.getColumns(), partition.getStorage()));
        result.setParameters(partition.getParameters());
        writeId.ifPresent(result::setWriteId);
        return result;
    }

    public static Database fromMetastoreApiDatabase(io.trino.hive.thrift.metastore.Database database)
    {
        String ownerName = "PUBLIC";
        PrincipalType ownerType = ROLE;
        if (database.getOwnerName() != null) {
            ownerName = database.getOwnerName();
            ownerType = fromMetastoreApiPrincipalType(database.getOwnerType());
        }

        Map<String, String> parameters = database.getParameters();
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }

        return Database.builder()
                .setDatabaseName(database.getName())
                // Some metastore implementations like Databricks Unity Catalog can return empty strings
                // for database locations. We treat them as null.
                .setLocation(Optional.ofNullable(emptyToNull(database.getLocationUri())))
                .setOwnerName(Optional.of(ownerName))
                .setOwnerType(Optional.of(ownerType))
                .setComment(Optional.ofNullable(database.getDescription()))
                .setParameters(parameters)
                .build();
    }

    public static Table fromMetastoreApiTable(io.trino.hive.thrift.metastore.Table table)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }
        return fromMetastoreApiTable(table, storageDescriptor.getCols());
    }

    public static Table fromMetastoreApiTable(io.trino.hive.thrift.metastore.Table table, List<FieldSchema> schema)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setOwner(Optional.ofNullable(table.getOwner()))
                .setTableType(table.getTableType())
                .setDataColumns(schema.stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toImmutableList()))
                .setPartitionColumns(table.getPartitionKeys().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toImmutableList()))
                .setParameters(table.getParameters() == null ? ImmutableMap.of() : table.getParameters())
                .setViewOriginalText(Optional.ofNullable(emptyToNull(table.getViewOriginalText())))
                .setViewExpandedText(Optional.ofNullable(emptyToNull(table.getViewExpandedText())))
                .setWriteId(table.getWriteId() < 0 ? OptionalLong.empty() : OptionalLong.of(table.getWriteId()));

        fromMetastoreApiStorageDescriptor(storageDescriptor, tableBuilder.getStorageBuilder(), table.getTableName());

        return tableBuilder.build();
    }

    public static boolean isAvroTableWithSchemaSet(io.trino.hive.thrift.metastore.Table table)
    {
        if (table.getParameters() == null) {
            return false;
        }
        SerDeInfo serdeInfo = getSerdeInfo(table);

        return serdeInfo.getSerializationLib() != null &&
                ((table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null ||
                        (serdeInfo.getParameters() != null && serdeInfo.getParameters().get(AVRO_SCHEMA_URL_KEY) != null)) ||
                 (table.getParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null ||
                         (serdeInfo.getParameters() != null && serdeInfo.getParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null))) &&
                serdeInfo.getSerializationLib().equals(AVRO.getSerde());
    }

    public static boolean isCsvTable(io.trino.hive.thrift.metastore.Table table)
    {
        return CSV.getSerde().equals(getSerdeInfo(table).getSerializationLib());
    }

    public static boolean isCsvPartition(io.trino.hive.thrift.metastore.Partition partition)
    {
        return CSV.getSerde().equals(getSerdeInfo(partition).getSerializationLib());
    }

    public static List<FieldSchema> csvSchemaFields(List<FieldSchema> schemas)
    {
        return schemas.stream()
                .map(schema -> new FieldSchema(schema.getName(), HiveType.HIVE_STRING.toString(), schema.getComment()))
                .collect(toImmutableList());
    }

    private static SerDeInfo getSerdeInfo(io.trino.hive.thrift.metastore.Table table)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table does not contain a storage descriptor: " + table);
        }
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        return serdeInfo;
    }

    private static SerDeInfo getSerdeInfo(io.trino.hive.thrift.metastore.Partition partition)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Partition does not contain a storage descriptor: " + partition);
        }
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Partition storage descriptor is missing SerDe info");
        }

        return serdeInfo;
    }

    public static Partition fromMetastoreApiPartition(io.trino.hive.thrift.metastore.Partition partition)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Partition does not contain a storage descriptor: " + partition);
        }
        List<FieldSchema> schema = storageDescriptor.getCols();
        if (schema == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Partition storage descriptor does not contain columns to derive a schema: " + partition);
        }

        return fromMetastoreApiPartition(partition, schema);
    }

    public static Partition fromMetastoreApiPartition(io.trino.hive.thrift.metastore.Partition partition, List<FieldSchema> schema)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Partition does not contain a storage descriptor: " + partition);
        }

        Partition.Builder partitionBuilder = Partition.builder()
                .setDatabaseName(partition.getDbName())
                .setTableName(partition.getTableName())
                .setValues(partition.getValues())
                .setColumns(schema.stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toImmutableList()))
                .setParameters(partition.getParameters());

        fromMetastoreApiStorageDescriptor(
                storageDescriptor,
                partitionBuilder.getStorageBuilder(),
                format("%s.%s", partition.getTableName(), partition.getValues()));

        return partitionBuilder.build();
    }

    public static HiveColumnStatistics fromMetastoreApiColumnStatistics(ColumnStatisticsObj columnStatistics)
    {
        if (columnStatistics.getStatsData().isSetLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getStatsData().getLongStats();
            OptionalLong min = longStatsData.isSetLowValue() ? OptionalLong.of(longStatsData.getLowValue()) : OptionalLong.empty();
            OptionalLong max = longStatsData.isSetHighValue() ? OptionalLong.of(longStatsData.getHighValue()) : OptionalLong.empty();
            OptionalLong nullsCount = longStatsData.isSetNumNulls() ? fromMetastoreNullsCount(longStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesWithNullCount = longStatsData.isSetNumDVs() ? OptionalLong.of(longStatsData.getNumDVs()) : OptionalLong.empty();
            return createIntegerColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
        }
        if (columnStatistics.getStatsData().isSetDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getStatsData().getDoubleStats();
            OptionalDouble min = doubleStatsData.isSetLowValue() ? OptionalDouble.of(doubleStatsData.getLowValue()) : OptionalDouble.empty();
            OptionalDouble max = doubleStatsData.isSetHighValue() ? OptionalDouble.of(doubleStatsData.getHighValue()) : OptionalDouble.empty();
            OptionalLong nullsCount = doubleStatsData.isSetNumNulls() ? fromMetastoreNullsCount(doubleStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesWithNullCount = doubleStatsData.isSetNumDVs() ? OptionalLong.of(doubleStatsData.getNumDVs()) : OptionalLong.empty();
            return createDoubleColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
        }
        if (columnStatistics.getStatsData().isSetDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getStatsData().getDecimalStats();
            Optional<BigDecimal> min = decimalStatsData.isSetLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty();
            Optional<BigDecimal> max = decimalStatsData.isSetHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = decimalStatsData.isSetNumNulls() ? fromMetastoreNullsCount(decimalStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesWithNullCount = decimalStatsData.isSetNumDVs() ? OptionalLong.of(decimalStatsData.getNumDVs()) : OptionalLong.empty();
            return createDecimalColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
        }
        if (columnStatistics.getStatsData().isSetDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getStatsData().getDateStats();
            Optional<LocalDate> min = dateStatsData.isSetLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty();
            Optional<LocalDate> max = dateStatsData.isSetHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = dateStatsData.isSetNumNulls() ? fromMetastoreNullsCount(dateStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesWithNullCount = dateStatsData.isSetNumDVs() ? OptionalLong.of(dateStatsData.getNumDVs()) : OptionalLong.empty();
            return createDateColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
        }
        if (columnStatistics.getStatsData().isSetBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getStatsData().getBooleanStats();
            OptionalLong trueCount = OptionalLong.empty();
            OptionalLong falseCount = OptionalLong.empty();
            // Impala 'COMPUTE STATS' writes 1 as the numTrue and -1 as the numFalse
            if (booleanStatsData.isSetNumTrues() && booleanStatsData.isSetNumFalses() && (booleanStatsData.getNumFalses() != -1)) {
                trueCount = OptionalLong.of(booleanStatsData.getNumTrues());
                falseCount = OptionalLong.of(booleanStatsData.getNumFalses());
            }
            return createBooleanColumnStatistics(
                    trueCount,
                    falseCount,
                    booleanStatsData.isSetNumNulls() ? fromMetastoreNullsCount(booleanStatsData.getNumNulls()) : OptionalLong.empty());
        }
        if (columnStatistics.getStatsData().isSetStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStatsData().getStringStats();
            OptionalLong maxColumnLength = stringStatsData.isSetMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = stringStatsData.isSetAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = stringStatsData.isSetNumNulls() ? fromMetastoreNullsCount(stringStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesWithNullCount = stringStatsData.isSetNumDVs() ? OptionalLong.of(stringStatsData.getNumDVs()) : OptionalLong.empty();
            return createStringColumnStatistics(
                    maxColumnLength,
                    averageColumnLength,
                    nullsCount,
                    distinctValuesWithNullCount);
        }
        if (columnStatistics.getStatsData().isSetBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getStatsData().getBinaryStats();
            OptionalLong maxColumnLength = binaryStatsData.isSetMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = binaryStatsData.isSetAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = binaryStatsData.isSetNumNulls() ? fromMetastoreNullsCount(binaryStatsData.getNumNulls()) : OptionalLong.empty();
            return createBinaryColumnStatistics(
                    maxColumnLength,
                    averageColumnLength,
                    nullsCount);
        }
        throw new TrinoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + columnStatistics);
    }

    private static Optional<LocalDate> fromMetastoreDate(Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(date.getDaysSinceEpoch()));
    }

    /**
     * Impala `COMPUTE STATS` will write -1 as the null count.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IMPALA-7497">IMPALA-7497</a>
     */
    public static OptionalLong fromMetastoreNullsCount(long nullsCount)
    {
        if (nullsCount == -1L) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(nullsCount);
    }

    private static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    public static Set<RoleGrant> fromRolePrincipalGrants(Collection<RolePrincipalGrant> grants)
    {
        return grants.stream().map(ThriftMetastoreUtil::fromRolePrincipalGrant).collect(toImmutableSet());
    }

    private static RoleGrant fromRolePrincipalGrant(RolePrincipalGrant grant)
    {
        return new RoleGrant(
                new TrinoPrincipal(fromMetastoreApiPrincipalType(grant.getPrincipalType()), grant.getPrincipalName()),
                grant.getRoleName(),
                grant.isGrantOption());
    }

    public static io.trino.hive.thrift.metastore.PrincipalType fromTrinoPrincipalType(PrincipalType principalType)
    {
        return switch (principalType) {
            case USER -> io.trino.hive.thrift.metastore.PrincipalType.USER;
            case ROLE -> io.trino.hive.thrift.metastore.PrincipalType.ROLE;
        };
    }

    public static PrincipalType fromMetastoreApiPrincipalType(io.trino.hive.thrift.metastore.PrincipalType principalType)
    {
        requireNonNull(principalType, "principalType is null");
        switch (principalType) {
            case USER:
                return USER;
            case ROLE:
                return ROLE;
            case GROUP:
                // TODO
                break;
        }
        throw new IllegalArgumentException("Unsupported principal type: " + principalType);
    }

    public static FieldSchema toMetastoreApiFieldSchema(Column column)
    {
        checkArgument(column.getProperties().isEmpty(), "Persisting column properties is not supported: %s", column);
        return new FieldSchema(column.getName(), column.getType().getHiveTypeName().toString(), column.getComment().orElse(null));
    }

    private static Column fromMetastoreApiFieldSchema(FieldSchema fieldSchema)
    {
        return new Column(fieldSchema.getName(), HiveType.valueOf(fieldSchema.getType().toLowerCase(ENGLISH)), Optional.ofNullable(fieldSchema.getComment()), ImmutableMap.of());
    }

    private static void fromMetastoreApiStorageDescriptor(
            StorageDescriptor storageDescriptor,
            Storage.Builder builder,
            String tablePartitionName)
    {
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        builder.setStorageFormat(StorageFormat.createNullable(serdeInfo.getSerializationLib(), storageDescriptor.getInputFormat(), storageDescriptor.getOutputFormat()))
                .setLocation(nullToEmpty(storageDescriptor.getLocation()))
                .setBucketProperty(createBucketProperty(storageDescriptor, tablePartitionName))
                .setSkewed(storageDescriptor.isSetSkewedInfo() && storageDescriptor.getSkewedInfo().isSetSkewedColNames() && !storageDescriptor.getSkewedInfo().getSkewedColNames().isEmpty())
                .setSerdeParameters(serdeInfo.getParameters() == null ? ImmutableMap.of() : serdeInfo.getParameters());
    }

    private static StorageDescriptor makeStorageDescriptor(String tableName, List<Column> columns, Storage storage)
    {
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(storage.getStorageFormat().getSerDeNullable());
        serdeInfo.setParameters(storage.getSerdeParameters());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(emptyToNull(storage.getOptionalLocation().orElse(null)));
        sd.setCols(columns.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiFieldSchema)
                .collect(toImmutableList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setSkewedInfoIsSet(storage.isSkewed());
        sd.setParameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.setNumBuckets(bucketProperty.get().bucketCount());
            sd.setBucketCols(bucketProperty.get().bucketedBy());
            if (!bucketProperty.get().sortedBy().isEmpty()) {
                sd.setSortCols(bucketProperty.get().sortedBy().stream()
                        .map(column -> new Order(column.columnName(), column.order().getHiveOrder()))
                        .collect(toImmutableList()));
            }
        }

        return sd;
    }

    private static Optional<HiveBucketProperty> createBucketProperty(StorageDescriptor storageDescriptor, String tablePartitionName)
    {
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }
        if (!bucketColsSet) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
        }
        // Ensure that the names used for columns are specified in lower case to match the names of the table columns
        List<SortingColumn> sortedBy = ImmutableList.of();
        if (storageDescriptor.isSetSortCols()) {
            sortedBy = storageDescriptor.getSortCols().stream()
                    .map(order -> new SortingColumn(
                            order.getCol().toLowerCase(ENGLISH),
                            SortingColumn.Order.fromMetastoreApiOrder(order.getOrder(), tablePartitionName)))
                    .collect(toImmutableList());
        }
        List<String> bucketColumnNames = storageDescriptor.getBucketCols().stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        return Optional.of(new HiveBucketProperty(bucketColumnNames, storageDescriptor.getNumBuckets(), sortedBy));
    }

    public static Set<HivePrivilegeInfo> parsePrivilege(PrivilegeGrantInfo userGrant, Optional<HivePrincipal> grantee)
    {
        boolean grantOption = userGrant.isGrantOption();
        String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
        HivePrincipal grantor = new HivePrincipal(fromMetastoreApiPrincipalType(userGrant.getGrantorType()), userGrant.getGrantor());
        return switch (name) {
            case "ALL" -> Arrays.stream(HivePrivilegeInfo.HivePrivilege.values())
                    .map(hivePrivilege -> new HivePrivilegeInfo(hivePrivilege, grantOption, grantor, grantee.orElse(grantor)))
                    .collect(toImmutableSet());
            case "SELECT" -> ImmutableSet.of(new HivePrivilegeInfo(SELECT, grantOption, grantor, grantee.orElse(grantor)));
            case "INSERT" -> ImmutableSet.of(new HivePrivilegeInfo(INSERT, grantOption, grantor, grantee.orElse(grantor)));
            case "UPDATE" -> ImmutableSet.of(new HivePrivilegeInfo(UPDATE, grantOption, grantor, grantee.orElse(grantor)));
            case "DELETE" -> ImmutableSet.of(new HivePrivilegeInfo(DELETE, grantOption, grantor, grantee.orElse(grantor)));
            case "OWNERSHIP" -> ImmutableSet.of(new HivePrivilegeInfo(OWNERSHIP, grantOption, grantor, grantee.orElse(grantor)));
            default -> throw new IllegalArgumentException("Unsupported privilege name: " + name);
        };
    }

    public static ColumnStatisticsObj createMetastoreColumnStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "unsupported type: %s", columnType);
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
                return createBooleanStatistics(columnName, columnType, statistics);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                return createLongStatistics(columnName, columnType, statistics);
            case FLOAT:
            case DOUBLE:
                return createDoubleStatistics(columnName, columnType, statistics);
            case STRING:
            case VARCHAR:
            case CHAR:
                return createStringStatistics(columnName, columnType, statistics);
            case DATE:
                return createDateStatistics(columnName, columnType, statistics);
            case BINARY:
                return createBinaryStatistics(columnName, columnType, statistics);
            case DECIMAL:
                return createDecimalStatistics(columnName, columnType, statistics);

            case TIMESTAMPLOCALTZ:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                // TODO support these, when we add support for these Hive types
            case VOID:
            case UNKNOWN:
                break;
        }
        throw new IllegalArgumentException(format("unsupported type: %s", columnType));
    }

    private static ColumnStatisticsObj createBooleanStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        BooleanColumnStatsData data = new BooleanColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
            booleanStatistics.getFalseCount().ifPresent(data::setNumFalses);
            booleanStatistics.getTrueCount().ifPresent(data::setNumTrues);
        });
        return new ColumnStatisticsObj(columnName, columnType.toString(), booleanStats(data));
    }

    private static ColumnStatisticsObj createLongStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        LongColumnStatsData data = new LongColumnStatsData();
        statistics.getIntegerStatistics().ifPresent(integerStatistics -> {
            integerStatistics.getMin().ifPresent(data::setLowValue);
            integerStatistics.getMax().ifPresent(data::setHighValue);
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getDistinctValuesWithNullCount().ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), longStats(data));
    }

    private static ColumnStatisticsObj createDoubleStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DoubleColumnStatsData data = new DoubleColumnStatsData();
        statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
            doubleStatistics.getMin().ifPresent(data::setLowValue);
            doubleStatistics.getMax().ifPresent(data::setHighValue);
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getDistinctValuesWithNullCount().ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), doubleStats(data));
    }

    private static ColumnStatisticsObj createStringStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        StringColumnStatsData data = new StringColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getDistinctValuesWithNullCount().ifPresent(data::setNumDVs);
        data.setMaxColLen(statistics.getMaxValueSizeInBytes().orElse(0));
        data.setAvgColLen(statistics.getAverageColumnLength().orElse(0));
        return new ColumnStatisticsObj(columnName, columnType.toString(), stringStats(data));
    }

    private static ColumnStatisticsObj createDateStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DateColumnStatsData data = new DateColumnStatsData();
        statistics.getDateStatistics().ifPresent(dateStatistics -> {
            dateStatistics.getMin().ifPresent(value -> data.setLowValue(toMetastoreDate(value)));
            dateStatistics.getMax().ifPresent(value -> data.setHighValue(toMetastoreDate(value)));
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getDistinctValuesWithNullCount().ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), dateStats(data));
    }

    private static ColumnStatisticsObj createBinaryStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        BinaryColumnStatsData data = new BinaryColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        data.setMaxColLen(statistics.getMaxValueSizeInBytes().orElse(0));
        data.setAvgColLen(statistics.getAverageColumnLength().orElse(0));
        return new ColumnStatisticsObj(columnName, columnType.toString(), binaryStats(data));
    }

    private static ColumnStatisticsObj createDecimalStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DecimalColumnStatsData data = new DecimalColumnStatsData();
        statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
            decimalStatistics.getMin().ifPresent(value -> data.setLowValue(toMetastoreDecimal(value)));
            decimalStatistics.getMax().ifPresent(value -> data.setHighValue(toMetastoreDecimal(value)));
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getDistinctValuesWithNullCount().ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), decimalStats(data));
    }

    private static Date toMetastoreDate(LocalDate date)
    {
        return new Date(date.toEpochDay());
    }

    public static Decimal toMetastoreDecimal(BigDecimal decimal)
    {
        return new Decimal(Shorts.checkedCast(decimal.scale()), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
    }

    public static Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE)) {
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) {
            // TODO (https://github.com/trinodb/trino/issues/5859) Add support for timestamp MIN_VALUE, MAX_VALUE
            return ImmutableSet.of(NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            // TODO Collect MIN,MAX once it is used by the optimizer
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of();
        }
        // Throwing here to make sure this method is updated when a new type is added in Hive connector
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) ||
                type.equals(DOUBLE) || type.equals(REAL) ||
                type instanceof DecimalType;
    }

    public static LanguageFunction fromMetastoreApiFunction(io.trino.hive.thrift.metastore.Function function)
    {
        LanguageFunction result = decodeFunction(function.getFunctionName(), function.getResourceUris());

        return new LanguageFunction(
                result.signatureToken(),
                result.sql(),
                result.path(),
                Optional.ofNullable(function.getOwnerName()));
    }

    public static io.trino.hive.thrift.metastore.Function toMetastoreApiFunction(String databaseName, String functionName, LanguageFunction function)
    {
        return new io.trino.hive.thrift.metastore.Function()
                .setDbName(databaseName)
                .setFunctionName(metastoreFunctionName(functionName, function.signatureToken()))
                .setClassName("TrinoFunction")
                .setFunctionType(FunctionType.JAVA)
                .setOwnerType(io.trino.hive.thrift.metastore.PrincipalType.USER)
                .setOwnerName(function.owner().orElse(null))
                .setResourceUris(toResourceUris(LANGUAGE_FUNCTION_CODEC.toJsonBytes(function)));
    }

    public static byte[] fromResourceUris(List<ResourceUri> resourceUris)
    {
        ByteArrayDataOutput bytes = ByteStreams.newDataOutput();
        for (ResourceUri resourceUri : resourceUris) {
            bytes.write(base64Url().decode(resourceUri.getUri()));
        }
        byte[] compressed = bytes.toByteArray();

        ZstdDecompressor decompressor = ZstdDecompressor.create();
        long size = decompressor.getDecompressedSize(compressed, 0, compressed.length);
        byte[] output = new byte[toIntExact(size)];
        decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length);
        return output;
    }

    public static LanguageFunction decodeFunction(String name, List<ResourceUri> uris)
    {
        try {
            return LANGUAGE_FUNCTION_CODEC.fromJson(fromResourceUris(uris));
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Failed to decode function: " + name, e);
        }
    }

    public static DataOperationType toDataOperationType(AcidOperation acidOperation)
    {
        return switch (acidOperation) {
            case INSERT -> DataOperationType.INSERT;
            case MERGE -> DataOperationType.UPDATE;
            default -> throw new IllegalStateException("No metastore operation for ACID operation " + acidOperation);
        };
    }

    public static boolean isAvroTableWithSchemaSet(Table table)
    {
        return AVRO.getSerde().equals(table.getStorage().getStorageFormat().getSerDeNullable()) &&
                ((table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null ||
                        (table.getStorage().getSerdeParameters().get(AVRO_SCHEMA_URL_KEY) != null)) ||
                 (table.getParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null ||
                         (table.getStorage().getSerdeParameters().get(AVRO_SCHEMA_LITERAL_KEY) != null)));
    }
}
