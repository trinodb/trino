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
package io.trino.plugin.opensearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.opensearch.DecoderDescriptor;
import io.trino.plugin.opensearch.ScanQueryPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import org.opensearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowDecoder
        implements Decoder
{
    private final String path;
    private final List<String> fieldNames;
    private final List<Decoder> decoders;

    public RowDecoder(String path, List<String> fieldNames, List<Decoder> decoders)
    {
        this.path = requireNonNull(path, "path is null");
        this.fieldNames = fieldNames;
        this.decoders = decoders;
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object data = getter.get();

        if (data == null) {
            output.appendNull();
        }
        else if (data instanceof Map) {
            ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                for (int i = 0; i < decoders.size(); i++) {
                    String field = fieldNames.get(i);
                    decoders.get(i).decode(hit, () -> ScanQueryPageSource.getField((Map<String, Object>) data, field), fieldBuilders.get(i));
                }
            });
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected object for field '%s' of type ROW: %s [%s]", path, data, data.getClass().getSimpleName()));
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;
        private final List<NameAndDescriptor> fields;

        @JsonCreator
        public Descriptor(String path, List<NameAndDescriptor> fields)
        {
            this.path = path;
            this.fields = fields;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public List<NameAndDescriptor> getFields()
        {
            return fields;
        }

        @Override
        public Decoder createDecoder()
        {
            return new RowDecoder(
                    path,
                    fields.stream()
                            .map(NameAndDescriptor::getName)
                            .collect(toImmutableList()),
                    fields.stream()
                            .map(field -> field.getDescriptor().createDecoder())
                            .collect(toImmutableList()));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (!(o instanceof Descriptor descriptor)) {
                return false;
            }
            return descriptor.path.equals(this.path)
                    && descriptor.fields.equals(this.fields);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path, fields);
        }
    }

    public static class NameAndDescriptor
    {
        private final String name;
        private final DecoderDescriptor descriptor;

        @JsonCreator
        public NameAndDescriptor(String name, DecoderDescriptor descriptor)
        {
            this.name = name;
            this.descriptor = descriptor;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public DecoderDescriptor getDescriptor()
        {
            return descriptor;
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
            NameAndDescriptor that = (NameAndDescriptor) o;
            return Objects.equals(this.name, that.name)
                    && Objects.equals(this.descriptor, that.descriptor);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, descriptor);
        }
    }
}
