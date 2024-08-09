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

import io.trino.parquet.Field;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.ParquetTypeUtils.FieldContext;
import io.trino.parquet.ParquetTypeUtils.TrinoFieldContext;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.schema.Type.ID;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnById;
import static java.util.Objects.requireNonNull;

public final class IcebergParquetColumnIOConverter
{
    private IcebergParquetColumnIOConverter() {}

    public static Optional<Field> constructField(Type type, ColumnIO columnIO, ColumnIdentity columnIdentity)
    {
        requireNonNull(columnIdentity, "columnIdentity is null");
        return ParquetTypeUtils.constructField(columnIO, new IcebergFieldContext(type, columnIdentity));
    }

    private static class IcebergFieldContext
            implements FieldContext
    {
        private final TrinoFieldContext context;
        private final ColumnIdentity identity;

        public IcebergFieldContext(Type type, ColumnIdentity identity)
        {
            this(new TrinoFieldContext(type), identity);
        }

        private IcebergFieldContext(TrinoFieldContext context, ColumnIdentity identity)
        {
            this.context = requireNonNull(context, "context is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public Type type()
        {
            return context.type();
        }

        @Override
        public IcebergFieldContext getArrayElement(ColumnIO elementColumnIO)
        {
            checkArgument(identity.getChildren().size() == 1, "Not an array: %s", identity);
            return new IcebergFieldContext(
                    context.getArrayElement(elementColumnIO),
                    validateColumn(getOnlyElement(identity.getChildren()), elementColumnIO));
        }

        @Override
        public IcebergFieldContext getMapKey(ColumnIO keyColumnIO)
        {
            checkArgument(identity.getChildren().size() == 2, "Not a map: %s", identity);
            return new IcebergFieldContext(
                    context.getMapKey(keyColumnIO),
                    validateColumn(identity.getChildren().get(0), keyColumnIO));
        }

        @Override
        public FieldContext getMapValue(ColumnIO valueColumnIO)
        {
            checkArgument(identity.getChildren().size() == 2, "Not a map: %s", identity);
            return new IcebergFieldContext(
                    context.getMapValue(valueColumnIO),
                    validateColumn(identity.getChildren().get(1), valueColumnIO));
        }

        @Override
        public FieldContext getRowField(int index)
        {
            checkArgument(index < identity.getChildren().size(), "Invalid field index: %s for: %s", index, identity);
            return new IcebergFieldContext(
                    context.getRowField(index),
                    identity.getChildren().get(index));
        }

        @Override
        public Optional<ColumnIO> lookupRowColumn(RowType.Field field, GroupColumnIO groupColumnIO)
        {
            return Optional.ofNullable(lookupColumnById(groupColumnIO, identity.getId()));
        }

        private ColumnIdentity validateColumn(ColumnIdentity identity, ColumnIO columnIO)
        {
            ID columnId = columnIO.getType().getId();
            checkArgument(columnId == null || identity.getId() == columnId.intValue(), "Iceberg column ID (%s) does not match Parquet field ID (%s)", identity.getId(), columnId);
            return identity;
        }
    }
}
