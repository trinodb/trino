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
package io.trino.plugin.hive.aws.athena;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.aws.athena.projection.Projection;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.Path;

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
import static io.trino.plugin.hive.aws.athena.projection.Projection.invalidProjectionMessage;
import static io.trino.plugin.hive.util.HiveUtil.escapePathName;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class PartitionProjection
{
    private static final Pattern PROJECTION_LOCATION_TEMPLATE_PLACEHOLDER_PATTERN = Pattern.compile("(\\$\\{[^}]+\\})");

    private final boolean enabled;

    private final Optional<String> storageLocationTemplate;
    private final Map<String, Projection> columnProjections;

    public PartitionProjection(boolean enabled, Optional<String> storageLocationTemplate, Map<String, Projection> columnProjections)
    {
        this.enabled = enabled;
        this.storageLocationTemplate = requireNonNull(storageLocationTemplate, "storageLocationTemplate is null");
        this.columnProjections = ImmutableMap.copyOf(requireNonNull(columnProjections, "columnProjections is null"));
    }

    public boolean isEnabled()
    {
        return enabled;
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
                .map(parts -> String.join(Path.SEPARATOR, parts))
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
        List<String> partitionValues = toPartitionValues(partitionName);
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(partitionValues)
                .setParameters(Map.of())
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(storageLocationTemplate
                                .map(template -> expandStorageLocationTemplate(
                                        template,
                                        table.getPartitionColumns().stream()
                                                .map(column -> column.getName()).collect(Collectors.toList()),
                                        partitionValues))
                                .orElseGet(() -> format("%s/%s/", table.getStorage().getLocation(), partitionName)))
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private String expandStorageLocationTemplate(String template, List<String> partitionColumns, List<String> partitionValues)
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
}
