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
package io.trino.plugin.hudi.compaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.List;
import java.util.Map;

public class HudiCompactionPlan
        extends SpecificRecordBase
        implements SpecificRecord
{
    private static final Schema SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"HoodieCompactionPlan\",\"namespace\":\"org.apache.hudi.avro.model\",\"fields\":[{\"name\":\"operations\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"HoodieCompactionOperation\",\"fields\":[{\"name\":\"baseInstantTime\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"deltaFilePaths\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null},{\"name\":\"dataFilePath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"fileId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"partitionPath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"metrics\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"double\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"bootstrapFilePath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}}],\"default\":null},{\"name\":\"extraMetadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"version\",\"type\":[\"int\",\"null\"],\"default\":1}]}");

    private static final SpecificData MODEL = new SpecificData();

    private List<HudiCompactionOperation> operations;
    private Map<String, String> extraMetadata;
    private Integer version;

    public HudiCompactionPlan() {}

    public HudiCompactionPlan(List<HudiCompactionOperation> operations, Map<String, String> extraMetadata, Integer version)
    {
        this.operations = ImmutableList.copyOf(operations);
        this.extraMetadata = ImmutableMap.copyOf(extraMetadata);
        this.version = version;
    }

    @Override
    public SpecificData getSpecificData()
    {
        return MODEL;
    }

    @Override
    public Schema getSchema()
    {
        return SCHEMA;
    }

    public List<HudiCompactionOperation> getOperations()
    {
        return operations;
    }

    public Map<String, String> getExtraMetadata()
    {
        return extraMetadata;
    }

    public Integer getVersion()
    {
        return version;
    }

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(int field)
    {
        return switch (field) {
            case 0:
                yield operations;
            case 1:
                yield extraMetadata;
            case 2:
                yield version;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field);
        };
    }

    // Used by DatumReader.  Applications should not call.
    @Override
    @SuppressWarnings(value = "unchecked")
    public void put(int field, Object value)
    {
        switch (field) {
            case 0:
                operations = ImmutableList.copyOf((List<HudiCompactionOperation>) value);
                break;
            case 1:
                extraMetadata = ImmutableMap.copyOf((Map<String, String>) value);
                break;
            case 2:
                version = (Integer) value;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field);
        }
    }
}
