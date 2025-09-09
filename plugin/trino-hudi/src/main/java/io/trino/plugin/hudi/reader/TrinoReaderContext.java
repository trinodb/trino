package io.trino.plugin.hudi.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.AvroJavaTypeConverter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class TrinoReaderContext
    extends RecordContext<IndexedRecord>
{

    public TrinoReaderContext(HoodieTableConfig tableConfig) {
        super(tableConfig, new AvroJavaTypeConverter());
    }

    @Override
    public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord, String partitionPath)
    {
        if (bufferedRecord.isDelete()) {
            return new HoodieEmptyRecord<>(
                    new HoodieKey(bufferedRecord.getRecordKey(), partitionPath),
                    HoodieRecord.HoodieRecordType.AVRO);
        }

        return new HoodieAvroIndexedRecord(bufferedRecord.getRecord());
    }

    public static Object getFieldValueFromIndexedRecord(
            IndexedRecord record,
            String fieldName) {
        Schema currentSchema = record.getSchema();
        IndexedRecord currentRecord = record;
        String[] path = fieldName.split("\\.");
        for (int i = 0; i < path.length; i++) {
            if (currentSchema.isUnion()) {
                currentSchema = AvroSchemaUtils.resolveNullableSchema(currentSchema);
            }
            Schema.Field field = currentSchema.getField(path[i]);
            if (field == null) {
                return null;
            }
            Object value = currentRecord.get(field.pos());
            if (i == path.length - 1) {
                return value;
            }
            currentSchema = field.schema();
            currentRecord = (IndexedRecord) value;
        }
        return null;
    }

    @Override
    public IndexedRecord mergeWithEngineRecord(Schema schema, Map<Integer, Object> updateValues, BufferedRecord<IndexedRecord> baseRecord)
    {
        IndexedRecord engineRecord = baseRecord.getRecord();
        for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
            engineRecord.put(value.getKey(), value.getValue());
        }
        return engineRecord;
    }

    @Override
    public IndexedRecord constructEngineRecord(Schema recordSchema, Object[] fieldValues)
    {
        GenericData.Record record = new GenericData.Record(recordSchema);
        for (int i = 0; i < fieldValues.length; i++) {
            record.put(i, fieldValues[i]);
        }
        return record;
    }

    @Override
    public Object getValue(IndexedRecord record, Schema schema, String fieldName)
    {
        return getFieldValueFromIndexedRecord(record, fieldName);
    }

    @Override
    public String getMetaFieldValue(IndexedRecord record, int pos)
    {
        return record.get(pos).toString();
    }

    @Override
    public IndexedRecord convertAvroRecord(IndexedRecord avroRecord)
    {
        return avroRecord;
    }

    @Override
    public GenericRecord convertToAvroRecord(IndexedRecord record, Schema schema)
    {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            genericRecord.put(field.name(), record.get(field.pos()));
        }
        return genericRecord;
    }

    @Nullable
    @Override
    public IndexedRecord getDeleteRow(String recordKey)
    {
        throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
    }

    @Override
    public IndexedRecord seal(IndexedRecord record)
    {
        // TODO: this can rely on colToPos map directly instead of schema
        Schema schema = record.getSchema();
        IndexedRecord newRecord = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            int pos = schema.getField(field.name()).pos();
            newRecord.put(pos, record.get(pos));
        }
        return newRecord;
    }

    @Override
    public IndexedRecord toBinaryRow(Schema avroSchema, IndexedRecord record)
    {
        return record;
    }

    @Override
    public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns)
    {
        List<Schema.Field> toFields = to.getFields();
        int[] projection = new int[toFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = from.getField(toFields.get(i).name()).pos();
        }

        return fromRecord -> {
            IndexedRecord toRecord = new GenericData.Record(to);
            for (int i = 0; i < projection.length; i++) {
                toRecord.put(i, fromRecord.get(projection[i]));
            }
            return toRecord;
        };
    }
}
