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
package io.trino.plugin.hidden.partitioning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiddenPartitioningPartitionSpec
{
    private final List<PartitionField> fields;

    @JsonCreator
    public HiddenPartitioningPartitionSpec(@JsonProperty("fields") List<PartitionField> fields)
    {
        this.fields = fields;
    }

    public List<PartitionField> getFields()
    {
        return fields;
    }

    public static class PartitionField
    {
        private final String sourceName;
        private final String name;
        private final String transform;

        @JsonCreator
        public PartitionField(
                @JsonProperty("source-name") String sourceName,
                @JsonProperty("name") String name,
                @JsonProperty("transform") String transform)
        {
            this.sourceName = sourceName;
            this.name = name;
            this.transform = transform;
        }

        public String getSourceName()
        {
            return sourceName;
        }

        public String getName()
        {
            return name;
        }

        public String getTransform()
        {
            return transform;
        }
    }
}
