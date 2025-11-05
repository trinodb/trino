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
package io.trino.plugin.hive.projection;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.cartesianProduct;
import static io.trino.metastore.Partitions.escapePathName;
import static io.trino.metastore.Partitions.toPartitionValues;
import static io.trino.plugin.hive.projection.InvalidProjectionException.invalidProjectionMessage;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class PartitionProjection
{
    private static final Pattern PROJECTION_LOCATION_TEMPLATE_PLACEHOLDER_PATTERN = Pattern.compile("(\\$\\{[^}]+\\})");

    private final Optional<String> storageLocationTemplate;
    private final boolean escapeInjectedColumnValues;
    private final Map<String, Projection> columnProjections;

    public PartitionProjection(Optional<String> storageLocationTemplate, Map<String, Projection> columnProjections)
    {
        this.storageLocationTemplate = requireNonNull(storageLocationTemplate, "storageLocationTemplate is null");
        this.escapeInjectedColumnValues = isEscapeInjectedColumnValues(storageLocationTemplate, columnProjections);
        this.columnProjections = ImmutableMap.copyOf(requireNonNull(columnProjections, "columnProjections is null"));
    }

    // Check if we need to escape injected column values in the storage location template.
    // The projection partition storage location template is compatible with Hive we need to escape injected column values.
    //
    // If the storage location template is not defined, we think that it is compatible -- we will use
    // the default partition location format (e.g. "col1=val1/col2=val2/")
    //
    // If the storage location is defined, we need to escape injected column values only if the template is compatible:
    // - it ends with the Hive default partition format (e.g. "col1=${col1}/col2=${col2}")
    // - the prefix of the template does not contain any column references (e.g. "${col1}" or "${col2}")
    private static boolean isEscapeInjectedColumnValues(Optional<String> storageLocationTemplate, Map<String, Projection> columnProjections)
    {
        if (storageLocationTemplate.isEmpty()) {
            return true;
        }

        String locationTemplate = storageLocationTemplate.get();
        if (locationTemplate.endsWith("/")) {
            locationTemplate = locationTemplate.substring(0, locationTemplate.length() - 1);
        }
        String hiveLocationTemplate = toPartitionLocationTemplate(columnProjections.keySet());
        if (!locationTemplate.endsWith(hiveLocationTemplate)) {
            return false;
        }
        String locationPrefix = locationTemplate.substring(0, locationTemplate.length() - hiveLocationTemplate.length());

        // prefix shouldn't reference any column at all
        return columnProjections.keySet().stream()
                .map(column -> "${" + column + "}")
                .noneMatch(locationPrefix::contains);
    }

    // TODO: support writing partition projection
    public void checkWriteSupported()
    {
        if (storageLocationTemplate.isPresent()) {
            if (!escapeInjectedColumnValues) {
                throw new TrinoException(NOT_SUPPORTED, "Partition projection with storage location template is not compatible with Hive");
            }
        }

        for (Projection projection : columnProjections.values()) {
            // DateProjection may contain a user-defined format that is incompatible with Hive.
            // Currently, we only support writing partitions in Hive's default date format.
            // Allowing custom date formats in partition projection can lead to inconsistencies
            // between reading and writing of the projection partition.
            // To ensure compatibility and avoid incorrect query results, disable writing for now.
            if (projection instanceof DateProjection dateProjection) {
                dateProjection.checkWriteSupported();
            }
        }
    }

    public Optional<List<String>> getProjectedPartitionNamesByFilter(List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        if (partitionKeysFilter.isNone()) {
            return Optional.empty();
        }
        Map<String, Domain> columnDomainMap = partitionKeysFilter.getDomains().orElseThrow(VerifyException::new);
        // Should not happen as we enforce defining partition projection for all partitioning columns.
        // But we leave a check as we might get wrong settings stored by 3rd party system.
        columnNames.forEach(columnName -> checkArgument(
                columnProjections.containsKey(columnName),
                invalidProjectionMessage(columnName, "Projection not defined for this column")));
        List<Set<String>> projectedPartitionValues = columnNames.stream()
                .map(columnName -> columnProjections.get(columnName)
                        .getProjectedValues(Optional.ofNullable(columnDomainMap.get(columnName)))
                        .stream()
                        // Partition names are effectively used as subfolder in underlining fs. So we need to escape illegal chars.
                        .map(projectedValue -> escapePathName(columnName) + "=" + escapePathName(projectedValue))
                        .collect(toImmutableSet()))
                .collect(toImmutableList());
        return Optional.of(cartesianProduct(projectedPartitionValues)
                .stream()
                .map(parts -> String.join("/", parts))
                .collect(toImmutableList()));
    }

    public Map<String, Optional<Partition>> getProjectedPartitionsByNames(Table table, List<String> partitionNames)
    {
        return partitionNames.stream()
                .collect(Collectors.toMap(
                        partitionName -> partitionName,
                        partitionName -> Optional.of(buildPartitionObject(table, partitionName))));
    }

    private Partition buildPartitionObject(Table table, String partitionName)
    {
        List<String> partitionColumns = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        List<String> partitionValues = toPartitionValues(partitionName);
        ImmutableList.Builder<String> partitionTemplateValues = ImmutableList.builderWithExpectedSize(partitionValues.size());
        for (int i = 0; i < partitionColumns.size(); i++) {
            String partitionColumn = partitionColumns.get(i);
            String partitionValue = columnProjections.get(partitionColumn).toPartitionLocationTemplateValue(partitionValues.get(i));
            partitionTemplateValues.add(escapeInjectedColumnValues ? escapePathName(partitionValue) : partitionValue);
        }

        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(partitionValues)
                .setParameters(Map.of())
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(expandStorageLocationTemplate(
                                storageLocationTemplate.orElseGet(() -> getPartitionLocation(table.getStorage().getLocation(), partitionColumns)),
                                partitionColumns,
                                partitionTemplateValues.build()))
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private static String getPartitionLocation(String tableLocation, List<String> partitionColumns)
    {
        if (tableLocation.endsWith("/")) {
            return format("%s%s/", tableLocation, toPartitionLocationTemplate(partitionColumns));
        }
        return format("%s/%s/", tableLocation, toPartitionLocationTemplate(partitionColumns));
    }

    private static String expandStorageLocationTemplate(String template, List<String> partitionColumns, List<String> partitionValues)
    {
        Matcher matcher = PROJECTION_LOCATION_TEMPLATE_PLACEHOLDER_PATTERN.matcher(template);
        StringBuilder location = new StringBuilder();
        while (matcher.find()) {
            String columnPlaceholder = matcher.group(1);
            String columnName = columnPlaceholder.substring(2, columnPlaceholder.length() - 1);
            matcher.appendReplacement(location, partitionValues.get(partitionColumns.indexOf(columnName)));
        }
        matcher.appendTail(location);
        return location.toString();
    }

    private static String toPartitionLocationTemplate(Collection<String> partitionColumns)
    {
        return partitionColumns.stream()
                .map(column -> column + "=${" + column + "}")
                .collect(Collectors.joining("/"));
    }
}
