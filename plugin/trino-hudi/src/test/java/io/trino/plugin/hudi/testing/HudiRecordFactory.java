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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating Hudi records with deterministic properties to ensure
 * reproducible test results.
 * <p>
 * This factory provides utilities for:
 * <ul>
 *   <li>Creating records with predictable record keys</li>
 *   <li>Generating deterministic UUIDs based on seeds</li>
 *   <li>Building Avro records from field values</li>
 * </ul>
 */
public class HudiRecordFactory
{
    private final Schema schema;
    private final Random random;

    /**
     * Creates a factory with the specified Avro schema.
     *
     * @param schema the Avro schema for the records
     */
    public HudiRecordFactory(Schema schema)
    {
        this(schema, 12345L);
    }

    /**
     * Creates a factory with the specified Avro schema and random seed.
     *
     * @param schema the Avro schema for the records
     * @param seed the seed for deterministic random number generation
     */
    public HudiRecordFactory(Schema schema, long seed)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.random = new Random(seed);
    }

    /**
     * Creates a Hudi record with the specified record key, partition path, and field values.
     *
     * @param recordKey the record key (e.g., "id:1")
     * @param partitionPath the partition path (e.g., "dt=2021-12-09/hh=10" or "" for non-partitioned)
     * @param fieldValues map of field names to values
     * @return a new HoodieRecord
     */
    public HoodieRecord<HoodieAvroPayload> createRecord(
            String recordKey,
            String partitionPath,
            Map<String, Object> fieldValues)
    {
        GenericRecord record = new GenericData.Record(schema);

        // Set all field values
        for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            // Verify field exists in schema
            if (schema.getField(fieldName) == null) {
                throw new IllegalArgumentException(
                        "Field '" + fieldName + "' does not exist in schema. Available fields: " + schema.getFields());
            }

            record.put(fieldName, value);
        }

        HoodieKey key = new HoodieKey(recordKey, partitionPath);
        HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(record));
        return new HoodieAvroRecord<>(key, payload, null);
    }

    /**
     * Creates a Hudi record for a non-partitioned table.
     *
     * @param recordKey the record key
     * @param fieldValues map of field names to values
     * @return a new HoodieRecord
     */
    public HoodieRecord<HoodieAvroPayload> createRecord(String recordKey, Map<String, Object> fieldValues)
    {
        return createRecord(recordKey, "", fieldValues);
    }

    /**
     * Generates a deterministic UUID based on the current random state.
     * <p>
     * This ensures file IDs and other UUID-based identifiers are reproducible
     * across test runs.
     *
     * @return a deterministic UUID
     */
    public UUID generateDeterministicUuid()
    {
        long mostSigBits = random.nextLong();
        long leastSigBits = random.nextLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Builder for creating field value maps with a fluent API.
     */
    public static class FieldValuesBuilder
    {
        private final Map<String, Object> values = new LinkedHashMap<>();

        /**
         * Adds a field with the specified name and value.
         */
        public FieldValuesBuilder field(String name, Object value)
        {
            values.put(name, value);
            return this;
        }

        /**
         * Adds a string field.
         */
        public FieldValuesBuilder stringField(String name, String value)
        {
            return field(name, value);
        }

        /**
         * Adds a long field.
         */
        public FieldValuesBuilder longField(String name, long value)
        {
            return field(name, value);
        }

        /**
         * Adds an integer field.
         */
        public FieldValuesBuilder intField(String name, int value)
        {
            return field(name, value);
        }

        /**
         * Adds a double field.
         */
        public FieldValuesBuilder doubleField(String name, double value)
        {
            return field(name, value);
        }

        /**
         * Adds a boolean field.
         */
        public FieldValuesBuilder booleanField(String name, boolean value)
        {
            return field(name, value);
        }

        /**
         * Builds the field values map.
         */
        public Map<String, Object> build()
        {
            return new LinkedHashMap<>(values);
        }
    }

    /**
     * Creates a new field values builder.
     */
    public static FieldValuesBuilder fieldValues()
    {
        return new FieldValuesBuilder();
    }

    /**
     * Returns the schema used by this factory.
     */
    public Schema getSchema()
    {
        return schema;
    }
}
