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
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

/**
 * Readers read a base column once, so all columns projected from it must agree on its type. A column resolved
 * from an older schema, such as an equality delete key nested in a since dropped field, can disagree with the
 * base column of the read schema. Such a base column is read with the union of the struct fields of all its
 * projections, and each column is reshaped back to its own type.
 */
record MergedBaseColumns(List<IcebergColumnHandle> readColumns, List<Optional<Function<Block, Block>>> reshapes)
{
    MergedBaseColumns
    {
        readColumns = ImmutableList.copyOf(requireNonNull(readColumns, "readColumns is null"));
        reshapes = ImmutableList.copyOf(requireNonNull(reshapes, "reshapes is null"));
    }

    public static MergedBaseColumns identity(List<IcebergColumnHandle> columns)
    {
        return new MergedBaseColumns(columns, nCopies(columns.size(), Optional.empty()));
    }

    public static MergedBaseColumns mergeBaseColumns(List<IcebergColumnHandle> columns)
    {
        // the first column of a base column defines its layout, fields missing from it are appended
        Map<Integer, TypedIdentity> baseColumns = new HashMap<>();
        for (IcebergColumnHandle column : columns) {
            TypedIdentity baseColumn = new TypedIdentity(column.getBaseColumnIdentity(), column.getBaseType());
            baseColumns.merge(column.getBaseColumnIdentity().getId(), baseColumn, MergedBaseColumns::union);
        }

        ImmutableList.Builder<IcebergColumnHandle> readColumns = ImmutableList.builder();
        ImmutableList.Builder<Optional<Function<Block, Block>>> reshapes = ImmutableList.builder();
        for (IcebergColumnHandle column : columns) {
            TypedIdentity baseColumn = baseColumns.get(column.getBaseColumnIdentity().getId());
            if (baseColumn.identity().equals(column.getBaseColumnIdentity()) && baseColumn.type().equals(column.getBaseType())) {
                readColumns.add(column);
                reshapes.add(Optional.empty());
                continue;
            }

            TypedIdentity readColumn = baseColumn;
            for (int fieldId : column.getPath()) {
                readColumn = readColumn.child(fieldId);
            }
            readColumns.add(new IcebergColumnHandle.Builder(column)
                    .baseColumnIdentity(baseColumn.identity())
                    .fieldType(baseColumn.type(), readColumn.type())
                    .build());
            reshapes.add(reshape(readColumn, new TypedIdentity(column.getColumnIdentity(), column.getType())));
        }
        return new MergedBaseColumns(readColumns.build(), reshapes.build());
    }

    public ConnectorPageSource reshape(ConnectorPageSource pageSource)
    {
        if (reshapes.stream().allMatch(Optional::isEmpty)) {
            return pageSource;
        }
        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        for (int channel = 0; channel < reshapes.size(); channel++) {
            transforms.column(channel, reshapes.get(channel));
        }
        return transforms.build(pageSource);
    }

    /**
     * Returns the schema extended with the fields of the base columns that are missing from it, for readers that
     * project the file through an Iceberg schema.
     */
    public static Schema readSchema(Schema schema, List<IcebergColumnHandle> baseColumns)
    {
        Map<Integer, NestedField> columns = new LinkedHashMap<>();
        schema.columns().forEach(column -> columns.put(column.fieldId(), column));
        boolean changed = false;
        for (IcebergColumnHandle baseColumn : baseColumns) {
            ColumnIdentity identity = baseColumn.getBaseColumnIdentity();
            NestedField column = columns.get(identity.getId());
            if (column != null && createColumnIdentity(column).equals(identity)) {
                continue;
            }
            if (column != null) {
                columns.put(identity.getId(), NestedField.from(column).ofType(icebergType(identity, baseColumn.getBaseType(), column.type())).build());
            }
            else if (schema.findField(identity.getName()) == null) {
                columns.put(identity.getId(), NestedField.optional(identity.getId(), identity.getName(), toIcebergType(baseColumn.getBaseType(), identity)));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, "Cannot read dropped column %s, its name is used by another column".formatted(identity.getName()));
            }
            changed = true;
        }
        if (!changed) {
            return schema;
        }
        return new Schema(ImmutableList.copyOf(columns.values()), schema.getAliases(), schema.identifierFieldIds());
    }

    private static TypedIdentity union(TypedIdentity left, TypedIdentity right)
    {
        if (left.identity().getTypeCategory() != STRUCT || right.identity().getTypeCategory() != STRUCT || left.equals(right)) {
            return left;
        }

        Map<Integer, Integer> rightChildIndexes = new LinkedHashMap<>();
        List<ColumnIdentity> rightChildren = right.identity().getChildren();
        for (int i = 0; i < rightChildren.size(); i++) {
            rightChildIndexes.put(rightChildren.get(i).getId(), i);
        }
        List<RowType.Field> leftFields = ((RowType) left.type()).getFields();
        List<RowType.Field> rightFields = ((RowType) right.type()).getFields();

        List<ColumnIdentity> children = new ArrayList<>();
        List<RowType.Field> fields = new ArrayList<>();
        List<ColumnIdentity> leftChildren = left.identity().getChildren();
        for (int i = 0; i < leftChildren.size(); i++) {
            TypedIdentity child = new TypedIdentity(leftChildren.get(i), leftFields.get(i).getType());
            Integer rightIndex = rightChildIndexes.remove(child.identity().getId());
            if (rightIndex != null) {
                child = union(child, new TypedIdentity(rightChildren.get(rightIndex), rightFields.get(rightIndex).getType()));
            }
            children.add(child.identity());
            fields.add(new RowType.Field(leftFields.get(i).getName(), child.type()));
        }
        for (int rightIndex : rightChildIndexes.values()) {
            children.add(rightChildren.get(rightIndex));
            fields.add(rightFields.get(rightIndex));
        }

        ColumnIdentity identity = left.identity();
        return new TypedIdentity(new ColumnIdentity(identity.getId(), identity.getName(), STRUCT, children), RowType.from(fields));
    }

    /**
     * Returns the Iceberg type with the layout of the column, keeping the fields of the schema type that it contains.
     */
    private static org.apache.iceberg.types.Type icebergType(ColumnIdentity identity, Type type, org.apache.iceberg.types.Type schemaType)
    {
        if (identity.getTypeCategory() != STRUCT || !schemaType.isStructType()) {
            return schemaType;
        }
        StructType schemaStruct = schemaType.asStructType();
        List<ColumnIdentity> children = identity.getChildren();
        List<Type> fieldTypes = type.getTypeParameters();
        ImmutableList.Builder<NestedField> fields = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            ColumnIdentity child = children.get(i);
            NestedField schemaField = schemaStruct.field(child.getId());
            if (schemaField == null) {
                fields.add(NestedField.optional(child.getId(), child.getName(), toIcebergType(fieldTypes.get(i), child)));
            }
            else {
                fields.add(NestedField.from(schemaField).ofType(icebergType(child, fieldTypes.get(i), schemaField.type())).build());
            }
        }
        return StructType.of(fields.build());
    }

    private static Optional<Function<Block, Block>> reshape(TypedIdentity read, TypedIdentity target)
    {
        if (read.type().equals(target.type())) {
            return Optional.empty();
        }
        if (read.identity().getTypeCategory() != STRUCT || target.identity().getTypeCategory() != STRUCT) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot read column %s as %s together with a column of a different schema version".formatted(target.identity().getName(), target.type()));
        }

        List<ColumnIdentity> targetChildren = target.identity().getChildren();
        List<RowType.Field> targetFields = ((RowType) target.type()).getFields();
        int[] fieldIndexes = new int[targetChildren.size()];
        List<Optional<Function<Block, Block>>> fieldReshapes = new ArrayList<>();
        for (int i = 0; i < targetChildren.size(); i++) {
            int fieldId = targetChildren.get(i).getId();
            fieldIndexes[i] = read.identity().getChildIndexByFieldId(fieldId);
            fieldReshapes.add(reshape(read.child(fieldId), new TypedIdentity(targetChildren.get(i), targetFields.get(i).getType())));
        }

        return Optional.of(block -> {
            List<Block> readFields = getRowFieldsFromBlock(block);
            Block[] fields = new Block[fieldIndexes.length];
            for (int i = 0; i < fieldIndexes.length; i++) {
                Block field = readFields.get(fieldIndexes[i]);
                fields[i] = fieldReshapes.get(i).map(fieldReshape -> fieldReshape.apply(field)).orElse(field);
            }
            return RowBlock.fromNotNullSuppressedFieldBlocks(block.getPositionCount(), getValidityBitmap(block), fields);
        });
    }

    private static Optional<long[]> getValidityBitmap(Block block)
    {
        if (!block.mayHaveNull()) {
            return Optional.empty();
        }
        long[] valueIsValid = new long[Bitmap.wordsForBits(block.getPositionCount())];
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                Bitmap.set(valueIsValid, 0, position);
            }
        }
        return Optional.of(valueIsValid);
    }

    private record TypedIdentity(ColumnIdentity identity, Type type)
    {
        private TypedIdentity
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(type, "type is null");
        }

        public TypedIdentity child(int fieldId)
        {
            int index = identity.getChildIndexByFieldId(fieldId);
            return new TypedIdentity(identity.getChildByFieldId(fieldId), type.getTypeParameters().get(index));
        }
    }
}
