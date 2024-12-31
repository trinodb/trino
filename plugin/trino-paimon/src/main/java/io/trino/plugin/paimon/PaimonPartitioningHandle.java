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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Trino {@link ConnectorPartitioningHandle}.
 */
public class PaimonPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final byte[] schema;

    @JsonCreator
    public PaimonPartitioningHandle(@JsonProperty("schema") byte[] schema)
    {
        this.schema = schema;
    }

    @JsonProperty
    public byte[] getSchema()
    {
        return schema;
    }

    public TableSchema getOriginalSchema()
    {
        try {
            return InstantiationUtil.deserializeObject(this.schema, getClass().getClassLoader());
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(schema);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonPartitioningHandle that = (PaimonPartitioningHandle) o;
        return Arrays.equals(schema, that.getSchema());
    }
}
