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
package io.trino.plugin.varada.dispatcher;

import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.spi.type.Type;

public record WarmupElementWriteMetadata(
        WarmUpElement warmUpElement,
        int connectorBlockIndex,
        Type type,
        SchemaTableColumn schemaTableColumn)
{
    public static Builder builder(WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        return new Builder()
                .warmUpElement(warmupElementWriteMetadata.warmUpElement())
                .connectorBlockIndex(warmupElementWriteMetadata.connectorBlockIndex())
                .type(warmupElementWriteMetadata.type())
                .schemaTableColumn(warmupElementWriteMetadata.schemaTableColumn());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "WarmupElementWriteMetadata{" +
                "warmUpElement=" + warmUpElement +
                ", connectorBlockIndex=" + connectorBlockIndex +
                ", type=" + type +
                ", schemaTableColumn=" + schemaTableColumn +
                '}';
    }

    public static class Builder
    {
        private WarmUpElement warmUpElement;
        private int connectorBlockIndex;
        private Type type;
        private SchemaTableColumn schemaTableColumn;

        private Builder()
        {
        }

        public Builder warmUpElement(WarmUpElement warmUpElement)
        {
            this.warmUpElement = warmUpElement;
            return this;
        }

        public Builder connectorBlockIndex(int connectorBlockIndex)
        {
            this.connectorBlockIndex = connectorBlockIndex;
            return this;
        }

        public Builder type(Type type)
        {
            this.type = type;
            return this;
        }

        public Builder schemaTableColumn(SchemaTableColumn schemaTableColumn)
        {
            this.schemaTableColumn = schemaTableColumn;
            return this;
        }

        public WarmupElementWriteMetadata build()
        {
            return new WarmupElementWriteMetadata(warmUpElement,
                    connectorBlockIndex,
                    type,
                    schemaTableColumn);
        }
    }
}
