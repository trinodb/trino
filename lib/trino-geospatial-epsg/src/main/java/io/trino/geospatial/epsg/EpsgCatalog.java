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
package io.trino.geospatial.epsg;

import io.airlift.compress.v3.zstd.ZstdDecompressor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

final class EpsgCatalog
{
    private static final int MAGIC = 0x54524745; // TRGE
    private static final int FORMAT_VERSION = 1;
    private static final String CATALOG_RESOURCE = "/io/trino/geospatial/epsg/epsg-catalog.bin";

    private final WktSection crsWkts;
    private final WktSection operationWkts;
    private final Map<Pair, int[]> operationCodesByPair;

    private EpsgCatalog(WktSection crsWkts, WktSection operationWkts, Map<Pair, int[]> operationCodesByPair)
    {
        this.crsWkts = requireNonNull(crsWkts, "crsWkts is null");
        this.operationWkts = requireNonNull(operationWkts, "operationWkts is null");
        this.operationCodesByPair = unmodifiableMap(new HashMap<>(requireNonNull(operationCodesByPair, "operationCodesByPair is null")));
    }

    public static EpsgCatalog loadDefault()
    {
        try (InputStream input = EpsgCatalog.class.getResourceAsStream(CATALOG_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("EPSG catalog resource not found: " + CATALOG_RESOURCE);
            }
            return read(input);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read EPSG catalog", e);
        }
    }

    public boolean hasCrsCode(int code)
    {
        return crsWkts.contains(code);
    }

    public boolean hasOperationCode(int code)
    {
        return operationWkts.contains(code);
    }

    public String getCrsWkt(int code)
    {
        return crsWkts.get(code);
    }

    public String getOperationWkt(int code)
    {
        return operationWkts.get(code);
    }

    public int[] getOperationCodes(int sourceSrid, int targetSrid)
    {
        return operationCodesByPair.getOrDefault(new Pair(sourceSrid, targetSrid), new int[0]);
    }

    public Set<String> crsCodes()
    {
        return crsWkts.codes();
    }

    public Set<String> operationCodes()
    {
        return operationWkts.codes();
    }

    private static EpsgCatalog read(InputStream input)
            throws IOException
    {
        try (DataInputStream data = new DataInputStream(new BufferedInputStream(input))) {
            int magic = data.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid EPSG catalog magic: " + magic);
            }
            int version = data.readInt();
            if (version != FORMAT_VERSION) {
                throw new IOException("Unsupported EPSG catalog version: " + version);
            }

            WktSection crsWkts = WktSection.read(data);
            WktSection operationWkts = WktSection.read(data);

            Map<Pair, int[]> operationCodesByPair = new HashMap<>();
            int pairCount = data.readInt();
            for (int i = 0; i < pairCount; i++) {
                int source = data.readInt();
                int target = data.readInt();
                int[] operationCodes = new int[data.readInt()];
                for (int j = 0; j < operationCodes.length; j++) {
                    operationCodes[j] = data.readInt();
                }
                operationCodesByPair.put(new Pair(source, target), operationCodes);
            }
            return new EpsgCatalog(crsWkts, operationWkts, operationCodesByPair);
        }
    }

    private static byte[] readBytes(DataInputStream input)
            throws IOException
    {
        byte[] bytes = new byte[input.readInt()];
        input.readFully(bytes);
        return bytes;
    }

    private static Set<String> stringCodes(int[] codes)
    {
        Set<String> result = new TreeSet<>();
        for (int code : codes) {
            result.add(Integer.toString(code));
        }
        return result;
    }

    private static final class WktSection
    {
        private final int[] codes;
        private final int[] blocks;
        private final int[] offsets;
        private final int[] lengths;
        private final int[] uncompressedLengths;
        private final byte[][] compressedBlocks;

        private WktSection(
                int[] codes,
                int[] blocks,
                int[] offsets,
                int[] lengths,
                int[] uncompressedLengths,
                byte[][] compressedBlocks)
        {
            this.codes = requireNonNull(codes, "codes is null");
            this.blocks = requireNonNull(blocks, "blocks is null");
            this.offsets = requireNonNull(offsets, "offsets is null");
            this.lengths = requireNonNull(lengths, "lengths is null");
            this.uncompressedLengths = requireNonNull(uncompressedLengths, "uncompressedLengths is null");
            this.compressedBlocks = requireNonNull(compressedBlocks, "compressedBlocks is null");
        }

        private static WktSection read(DataInputStream data)
                throws IOException
        {
            int entryCount = data.readInt();
            int[] codes = new int[entryCount];
            int[] blocks = new int[entryCount];
            int[] offsets = new int[entryCount];
            int[] lengths = new int[entryCount];
            for (int i = 0; i < entryCount; i++) {
                codes[i] = data.readInt();
                blocks[i] = data.readInt();
                offsets[i] = data.readInt();
                lengths[i] = data.readInt();
            }

            int blockCount = data.readInt();
            int[] uncompressedLengths = new int[blockCount];
            byte[][] compressedBlocks = new byte[blockCount][];
            for (int i = 0; i < blockCount; i++) {
                uncompressedLengths[i] = data.readInt();
                compressedBlocks[i] = readBytes(data);
            }
            return new WktSection(codes, blocks, offsets, lengths, uncompressedLengths, compressedBlocks);
        }

        private boolean contains(int code)
        {
            return Arrays.binarySearch(codes, code) >= 0;
        }

        private String get(int code)
        {
            int index = Arrays.binarySearch(codes, code);
            if (index < 0) {
                return null;
            }
            byte[] block = decompressBlock(blocks[index]);
            return new String(block, offsets[index], lengths[index], UTF_8);
        }

        private Set<String> codes()
        {
            return stringCodes(codes);
        }

        private byte[] decompressBlock(int block)
        {
            byte[] output = new byte[uncompressedLengths[block]];
            int decompressedLength = ZstdDecompressor.create().decompress(compressedBlocks[block], 0, compressedBlocks[block].length, output, 0, output.length);
            if (decompressedLength != output.length) {
                throw new EpsgException("Invalid EPSG catalog block length. Expected %s bytes but got %s".formatted(output.length, decompressedLength));
            }
            return output;
        }
    }

    private record Pair(int source, int target) {}
}
