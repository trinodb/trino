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
package io.trino.plugin.elasticsearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GeopointDecoder
        implements Decoder
{
    private final String path;

    public GeopointDecoder(String path)
    {
        this.path = requireNonNull(path, "path is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String) {
            VARCHAR.writeSlice(output, Slices.utf8Slice(value.toString()));
        }
        else if (value instanceof ArrayList) {
            List<Double> valueArrayList = new ArrayList<>();
            for (Object o : (List<?>) value) {
                valueArrayList.add(Double.class.cast(o));
            }
            String changeArrayValueToString = "" + valueArrayList.get(0) + "," + valueArrayList.get(1);
            VARCHAR.writeSlice(output, Slices.utf8Slice(changeArrayValueToString.toString()));
        }
        else if (value instanceof Object) {
            ObjectMapper oMapper = new ObjectMapper();
            Map<String, Object> mapValue = oMapper.convertValue(value, Map.class);
            String changeObjectValueToString = "" + mapValue.get("lat") + "," + mapValue.get("lon");
            VARCHAR.writeSlice(output, Slices.utf8Slice(changeObjectValueToString.toString()));
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Unsupported representation for field '%s' of type VARCHAR: %s [%s]", path, value, value.getClass().getSimpleName()));
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;

        @JsonCreator
        public Descriptor(String path)
        {
            this.path = path;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @Override
        public Decoder createDecoder()
        {
            return new GeopointDecoder(path);
        }
    }
}
