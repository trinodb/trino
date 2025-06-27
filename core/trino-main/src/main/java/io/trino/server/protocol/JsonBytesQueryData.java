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
package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.trino.client.QueryData;
import io.trino.server.protocol.JsonEncodingUtils.TypeEncoder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.server.protocol.JsonEncodingUtils.writePagesToJsonGenerator;
import static java.util.Objects.requireNonNull;

public class JsonBytesQueryData
        implements QueryData
{
    private final TypeEncoder[] typeEncoders;
    private final int[] sourcePageChannels;
    private final List<Page> pages;
    private final Consumer<TrinoException> exceptionHandler;

    public JsonBytesQueryData(Consumer<TrinoException> exceptionHandler, TypeEncoder[] typeEncoders, int[] sourcePageChannels, List<Page> pages)
    {
        this.exceptionHandler = requireNonNull(exceptionHandler, "exceptionHandler is null");
        this.typeEncoders = requireNonNull(typeEncoders, "typeEncoders is null");
        this.sourcePageChannels = requireNonNull(sourcePageChannels, "sourcePageChannels is null");
        this.pages = ImmutableList.copyOf(pages);
    }

    public void writeTo(JsonGenerator generator)
    {
        writePagesToJsonGenerator(exceptionHandler, generator, typeEncoders, sourcePageChannels, pages);
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public long getRowsCount()
    {
        return pages.stream()
                .mapToLong(Page::getPositionCount)
                .sum();
    }
}
