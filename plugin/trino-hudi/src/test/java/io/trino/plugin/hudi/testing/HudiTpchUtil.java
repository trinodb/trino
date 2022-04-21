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

package io.trino.plugin.hudi.testing;

import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchColumnTypes;
import io.trino.tpch.TpchTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableList;

public class HudiTpchUtil
{
    static final String FIELD_UUID = "uuid";
    static final String PARTITION_PATH = "";

    private HudiTpchUtil() {}

    static RecordConverter createRecordConverter(TpchTable<?> table)
    {
        Schema schema = createArvoSchema(table);
        List<? extends TpchColumn<?>> columns = table.getColumns();

        final int columnNum = columns.size();
        List<String> columnNames = columns.stream()
                .map(TpchColumn::getSimplifiedColumnName)
                .collect(toUnmodifiableList());
        List<Function<Object, Object>> columnConverters = columns.stream()
                .map(TpchColumn::getType)
                .map(HudiTpchUtil::avroEncoderOf)
                .collect(toUnmodifiableList());

        return row -> {
            TpchTable<?> t = table;
            checkArgument(row.size() == columnNum);

            // Create a GenericRecord
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < columnNum; i++) {
                record.put(columnNames.get(i), columnConverters.get(i).apply(row.get(i)));
            }
            // Add extra uuid column
            String uuid = UUID.randomUUID().toString();
            record.put(FIELD_UUID, uuid);

            // wrap to a HoodieRecord
            HoodieKey key = new HoodieKey(uuid, PARTITION_PATH);
            HoodieAvroPayload data = new HoodieAvroPayload(Option.of(record));
            return new HoodieRecord<>(key, data);
        };
    }

    static Schema createArvoSchema(TpchTable<?> table)
    {
        List<? extends TpchColumn<?>> tpchColumns = table.getColumns();
        List<Schema.Field> fields = new ArrayList<>(tpchColumns.size() + 1);
        for (TpchColumn<?> column : tpchColumns) {
            String columnName = column.getSimplifiedColumnName();
            Schema.Type columnSchemaType = toSchemaType(column.getType());
            // Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
            fields.add(new Schema.Field(columnName, Schema.create(columnSchemaType)));
        }
        fields.add(new Schema.Field(FIELD_UUID, Schema.create(Schema.Type.STRING)));
        String name = table.getTableName();
        return Schema.createRecord(name, null, null, false, fields);
    }

    static List<Column> createMetastoreColumns(TpchTable<?> table)
    {
        List<? extends TpchColumn<?>> tpchColumns = table.getColumns();
        List<Column> columns = new ArrayList<>(tpchColumns.size() + 1);
        for (TpchColumn<?> c : tpchColumns) {
            columns.add(new Column(c.getSimplifiedColumnName(), toHiveType(c.getType()), Optional.empty()));
        }
        columns.add(new Column(FIELD_UUID, HiveType.HIVE_STRING, Optional.empty()));
        return unmodifiableList(columns);
    }

    private static Schema.Type toSchemaType(TpchColumnType columnType)
    {
        return TpchColumnTypeAdapter.of(columnType).avroType;
    }

    private static HiveType toHiveType(TpchColumnType columnType)
    {
        return TpchColumnTypeAdapter.of(columnType).hiveType;
    }

    private static Function<Object, Object> avroEncoderOf(TpchColumnType columnType)
    {
        return TpchColumnTypeAdapter.of(columnType).avroEncoder;
    }

    private enum TpchColumnTypeAdapter
    {
        INTEGER(Schema.Type.INT, HiveType.HIVE_INT, Function.identity()),
        IDENTIFIER(Schema.Type.LONG, HiveType.HIVE_LONG, Function.identity()),
        DATE(Schema.Type.INT, HiveType.HIVE_INT, TpchColumnTypeAdapter::convertDate),
        DOUBLE(Schema.Type.DOUBLE, HiveType.HIVE_DOUBLE, Function.identity()),
        VARCHAR(Schema.Type.STRING, HiveType.HIVE_STRING, Function.identity()),
        /**/;

        static TpchColumnTypeAdapter of(TpchColumnType columnType)
        {
            if (columnType == TpchColumnTypes.INTEGER) {
                return INTEGER;
            }
            else if (columnType == TpchColumnTypes.IDENTIFIER) {
                return IDENTIFIER;
            }
            else if (columnType == TpchColumnTypes.DATE) {
                return DATE;
            }
            else if (columnType == TpchColumnTypes.DOUBLE) {
                return DOUBLE;
            }
            else {
                if (columnType.getBase() != TpchColumnType.Base.VARCHAR || columnType.getPrecision().isEmpty()) {
                    throw new IllegalArgumentException("Illegal column type: " + columnType);
                }
                return VARCHAR;
            }
        }

        private final Schema.Type avroType;
        private final HiveType hiveType;
        private final Function<Object, Object> avroEncoder;

        TpchColumnTypeAdapter(Schema.Type avroType, HiveType hiveType, Function<Object, Object> avroEncoder)
        {
            this.avroType = avroType;
            this.hiveType = hiveType;
            this.avroEncoder = avroEncoder;
        }

        private static Object convertDate(Object input)
        {
            LocalDate date = (LocalDate) input;
            return (int) date.getLong(ChronoField.EPOCH_DAY);
        }
    }
}
