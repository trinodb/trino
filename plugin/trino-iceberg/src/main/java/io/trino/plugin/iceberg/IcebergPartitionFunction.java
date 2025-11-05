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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.PartitionFields.ICEBERG_BUCKET_PATTERN;
import static io.trino.plugin.iceberg.PartitionFields.ICEBERG_TRUNCATE_PATTERN;
import static java.util.Objects.requireNonNull;

// NOTE: the partitioning function must contain the data path for nested fields because the partitioning columns
// reference the top level column, not the nested column. This means that even thought the partitioning functions
// with a different path should be compatible, the system does not consider them compatible. Fortunately, partitioning
// on nested columns is not common.
public record IcebergPartitionFunction(Transform transform, List<Integer> dataPath, Type type, OptionalInt size)
{
    public enum Transform
    {
        IDENTITY,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        VOID,
        BUCKET,
        TRUNCATE
    }

    public IcebergPartitionFunction(Transform transform, List<Integer> dataPath, Type type)
    {
        this(transform, dataPath, type, OptionalInt.empty());
    }

    public IcebergPartitionFunction
    {
        requireNonNull(transform, "transform is null");
        requireNonNull(dataPath, "dataPath is null");
        checkArgument(!dataPath.isEmpty(), "dataPath is empty");
        requireNonNull(type, "type is null");
        requireNonNull(size, "size is null");
        checkArgument(size.orElse(0) >= 0, "size must be greater than or equal to zero");
        checkArgument(size.isEmpty() || transform == Transform.BUCKET || transform == Transform.TRUNCATE, "size is only valid for BUCKET and TRUNCATE transforms");
    }

    public IcebergPartitionFunction withTopLevelColumnIndex(int newColumnIndex)
    {
        return new IcebergPartitionFunction(
                transform,
                ImmutableList.<Integer>builder()
                        .add(newColumnIndex)
                        .addAll(dataPath().subList(1, dataPath().size()))
                        .build(),
                type,
                size);
    }

    public static IcebergPartitionFunction create(String transform, List<Integer> dataPath, Type type)
    {
        return switch (transform) {
            case "identity" -> new IcebergPartitionFunction(Transform.IDENTITY, dataPath, type);
            case "year" -> new IcebergPartitionFunction(Transform.YEAR, dataPath, type);
            case "month" -> new IcebergPartitionFunction(Transform.MONTH, dataPath, type);
            case "day" -> new IcebergPartitionFunction(Transform.DAY, dataPath, type);
            case "hour" -> new IcebergPartitionFunction(Transform.HOUR, dataPath, type);
            case "void" -> new IcebergPartitionFunction(Transform.VOID, dataPath, type);
            default -> {
                Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
                if (matcher.matches()) {
                    yield new IcebergPartitionFunction(Transform.BUCKET, dataPath, type, OptionalInt.of(Integer.parseInt(matcher.group(1))));
                }

                matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
                if (matcher.matches()) {
                    yield new IcebergPartitionFunction(Transform.TRUNCATE, dataPath, type, OptionalInt.of(Integer.parseInt(matcher.group(1))));
                }

                throw new UnsupportedOperationException("Unsupported partition transform: " + transform);
            }
        };
    }
}
