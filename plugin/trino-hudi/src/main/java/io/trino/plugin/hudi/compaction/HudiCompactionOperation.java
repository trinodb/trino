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
import org.apache.avro.Schema.Parser;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HudiCompactionOperation
        extends SpecificRecordBase
        implements SpecificRecord
{
    private static final Schema SCHEMA = new Parser().parse("{\"type\":\"record\",\"name\":\"HoodieCompactionOperation\",\"namespace\":\"org.apache.hudi.avro.model\",\"fields\":[{\"name\":\"baseInstantTime\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"deltaFilePaths\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"default\":null},{\"name\":\"dataFilePath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"fileId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"partitionPath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"metrics\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"double\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"bootstrapFilePath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");
    private static final SpecificData MODEL = new SpecificData();

    private String baseInstantTime;
    private List<String> deltaFilePaths;
    private String dataFilePath;
    private String fileId;
    private String partitionPath;
    private Map<String, Double> metrics;
    private String bootstrapFilePath;

    public HudiCompactionOperation() {}

    public HudiCompactionOperation(
            String baseInstantTime,
            List<String> deltaFilePaths,
            String dataFilePath,
            String fileId,
            String partitionPath,
            Map<String, Double> metrics,
            String bootstrapFilePath)
    {
        this.baseInstantTime = requireNonNull(baseInstantTime, "baseInstantTime is null");
        this.deltaFilePaths = requireNonNull(deltaFilePaths, "deltaFilePaths is null");
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.fileId = requireNonNull(fileId, "fileId is null");
        this.partitionPath = requireNonNull(partitionPath, "partitionPath is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.bootstrapFilePath = requireNonNull(bootstrapFilePath, "bootstrapFilePath is null");
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

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(int field)
    {
        return switch (field) {
            case 0:
                yield baseInstantTime;
            case 1:
                yield deltaFilePaths;
            case 2:
                yield dataFilePath;
            case 3:
                yield fileId;
            case 4:
                yield partitionPath;
            case 5:
                yield metrics;
            case 6:
                yield bootstrapFilePath;
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
                baseInstantTime = value != null ? value.toString() : null;
                break;
            case 1:
                deltaFilePaths = (List<String>) value;
                break;
            case 2:
                dataFilePath = value != null ? value.toString() : null;
                break;
            case 3:
                fileId = value != null ? value.toString() : null;
                break;
            case 4:
                partitionPath = value != null ? value.toString() : null;
                break;
            case 5:
                metrics = (Map<String, Double>) value;
                break;
            case 6:
                bootstrapFilePath = value != null ? value.toString() : null;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field);
        }
    }

    public String getBaseInstantTime()
    {
        return baseInstantTime;
    }

    public List<String> getDeltaFilePaths()
    {
        return deltaFilePaths;
    }

    public String getDataFilePath()
    {
        return dataFilePath;
    }

    public String getFileId()
    {
        return fileId;
    }

    public String getPartitionPath()
    {
        return partitionPath;
    }

    public Map<String, Double> getMetrics()
    {
        return metrics;
    }

    public String getBootstrapFilePath()
    {
        return bootstrapFilePath;
    }

    public static HudiCompactionOperation.Builder newBuilder()
    {
        return new HudiCompactionOperation.Builder();
    }

    public static class Builder
    {
        private String baseInstantTime;
        private List<String> deltaFilePaths;
        private String dataFilePath;
        private String fileId;
        private String partitionPath;
        private Map<String, Double> metrics;
        private String bootstrapFilePath;

        private Builder()
        {
        }

        public HudiCompactionOperation.Builder setBaseInstantTime(String baseInstantTime)
        {
            this.baseInstantTime = baseInstantTime;
            return this;
        }

        public HudiCompactionOperation.Builder setDeltaFilePaths(List<String> deltaFilePaths)
        {
            this.deltaFilePaths = ImmutableList.copyOf(deltaFilePaths);
            return this;
        }

        public HudiCompactionOperation.Builder setDataFilePath(String dataFilePath)
        {
            this.dataFilePath = dataFilePath;
            return this;
        }

        public HudiCompactionOperation.Builder setFileId(String fileId)
        {
            this.fileId = fileId;
            return this;
        }

        public HudiCompactionOperation.Builder setPartitionPath(String partitionPath)
        {
            this.partitionPath = partitionPath;
            return this;
        }

        public HudiCompactionOperation.Builder setMetrics(Map<String, Double> metrics)
        {
            this.metrics = ImmutableMap.copyOf(metrics);
            return this;
        }

        @SuppressWarnings("unchecked")
        public HudiCompactionOperation build()
        {
            return new HudiCompactionOperation(
                    baseInstantTime,
                    deltaFilePaths,
                    dataFilePath,
                    fileId,
                    partitionPath,
                    metrics,
                    bootstrapFilePath);
        }
    }
}
