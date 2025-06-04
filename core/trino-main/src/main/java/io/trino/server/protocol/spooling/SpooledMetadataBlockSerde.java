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
package io.trino.server.protocol.spooling;

import io.airlift.json.JsonCodec;
import io.trino.client.spooling.DataAttributes;
import io.trino.spi.Page;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Page layout: row(
 *  0: VARCHAR    metadata
 *  1: VARBINARY  inline data
 *  2: VARCHAR    identifier
 *  3: VARCHAR    direct location
 *  4: VARCHAR    headers)
 */
public class SpooledMetadataBlockSerde
{
    private static final JsonCodec<Map<String, List<String>>> HEADERS_CODEC = mapJsonCodec(String.class, listJsonCodec(String.class));
    private static final JsonCodec<DataAttributes> ATTRIBUTES_CODEC = JsonCodec.jsonCodec(DataAttributes.class);

    private static final RowType SPOOLING_METADATA_TYPE = RowType.from(List.of(
            new RowType.Field(Optional.empty(), VARCHAR),
            new RowType.Field(Optional.empty(), VARBINARY),
            new RowType.Field(Optional.empty(), VARCHAR),
            new RowType.Field(Optional.empty(), VARCHAR),
            new RowType.Field(Optional.empty(), VARCHAR)));

    private SpooledMetadataBlockSerde() {}

    public static Page serialize(SpooledMetadataBlock block)
    {
        RowBlockBuilder rowBlockBuilder = SPOOLING_METADATA_TYPE.createBlockBuilder(null, 1);
        rowBlockBuilder.buildEntry(rowEntryBuilder -> {
            VARCHAR.writeSlice(rowEntryBuilder.get(0), utf8Slice(ATTRIBUTES_CODEC.toJson(block.attributes())));
            switch (block) {
                case SpooledMetadataBlock.Inlined inlined -> {
                    VARBINARY.writeSlice(rowEntryBuilder.get(1), inlined.data());
                    rowEntryBuilder.get(2).appendNull();
                    rowEntryBuilder.get(3).appendNull();
                    rowEntryBuilder.get(4).appendNull();
                }
                case SpooledMetadataBlock.Spooled spooled -> {
                    rowEntryBuilder.get(1).appendNull();
                    VARCHAR.writeSlice(rowEntryBuilder.get(2), utf8Slice(spooled.identifier().toStringUtf8()));
                    if (spooled.directUri().isPresent()) {
                        VARCHAR.writeSlice(rowEntryBuilder.get(3), utf8Slice(spooled.directUri().orElseThrow().toString()));
                    }
                    else {
                        rowEntryBuilder.get(3).appendNull();
                    }
                    VARCHAR.writeSlice(rowEntryBuilder.get(4), utf8Slice(HEADERS_CODEC.toJson(spooled.headers())));
                }
            }
        });
        return new Page(rowBlockBuilder.build());
    }

    public static SpooledMetadataBlock deserialize(Page page)
    {
        verify(page.getPositionCount() == 1, "Spooling metadata block must have a single position");
        verify(page.getChannelCount() == 1, "Spooling metadata block must have a single channel");

        SqlRow row = SPOOLING_METADATA_TYPE.getObject(page.getBlock(0), 0);
        DataAttributes dataAttributes = ATTRIBUTES_CODEC.fromJson(VARCHAR.getSlice(row.getRawFieldBlock(0), 0).getInput());

        if (row.getRawFieldBlock(1).isNull(0)) {
            return new SpooledMetadataBlock.Spooled(
                    dataAttributes,
                    VARCHAR.getSlice(row.getRawFieldBlock(2), 0),
                    extractDirectUri(row),
                    HEADERS_CODEC.fromJson(VARCHAR.getSlice(row.getRawFieldBlock(4), 0).getInput()));
        }

        return new SpooledMetadataBlock.Inlined(dataAttributes, VARBINARY.getSlice(row.getRawFieldBlock(1), 0));
    }

    private static Optional<URI> extractDirectUri(SqlRow row)
    {
        if (row.getRawFieldBlock(3).isNull(0)) {
            return Optional.empty();
        }
        return Optional.of(URI.create(VARCHAR.getSlice(row.getRawFieldBlock(3), 0).toStringUtf8()));
    }
}
