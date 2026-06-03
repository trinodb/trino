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
package io.trino.geospatial.epsg.generator;

import io.airlift.compress.v3.zstd.ZstdCompressor;
import org.apache.sis.io.wkt.Convention;
import org.apache.sis.io.wkt.WKTFormat;
import org.apache.sis.referencing.CRS;
import org.opengis.metadata.Identifier;
import org.opengis.referencing.AuthorityFactory;
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.CoordinateOperationAuthorityFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;

import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class GenerateEpsgCatalog
{
    private static final int FORMAT_VERSION = 1;
    private static final int WKT_BLOCK_RECORD_COUNT = 64;
    private static final String CATALOG_RESOURCE = "io/trino/geospatial/epsg/epsg-catalog.bin";

    private GenerateEpsgCatalog() {}

    public static void main(String[] args)
            throws Exception
    {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: GenerateEpsgCatalog <generated-resources-directory>");
        }

        Path outputDirectory = Path.of(args[0]);
        Path resourceDirectory = outputDirectory.resolve("io/trino/geospatial/epsg");
        Files.createDirectories(resourceDirectory);
        Files.deleteIfExists(resourceDirectory.resolve("epsg-catalog.bin.gz"));

        AuthorityFactory authorityFactory = CRS.getAuthorityFactory("EPSG");
        if (!(authorityFactory instanceof CRSAuthorityFactory crsFactory)) {
            throw new IllegalStateException("EPSG authority factory does not support CRS lookup");
        }
        if (!(authorityFactory instanceof CoordinateOperationAuthorityFactory operationFactory)) {
            throw new IllegalStateException("EPSG authority factory does not support coordinate-operation lookup");
        }

        WKTFormat format = new WKTFormat(Locale.ROOT, (ZoneId) null);
        format.setConvention(Convention.WKT2_2019);
        format.setIndentation(WKTFormat.SINGLE_LINE);

        Map<Integer, byte[]> crsWkts = new TreeMap<>();
        int skippedCrs = 0;
        for (String code : crsFactory.getAuthorityCodes(CoordinateReferenceSystem.class)) {
            OptionalInt epsgCode = parseEpsgCode(code);
            if (epsgCode.isEmpty()) {
                skippedCrs++;
                continue;
            }
            try {
                CoordinateReferenceSystem crs = crsFactory.createCoordinateReferenceSystem(code);
                crsWkts.put(epsgCode.getAsInt(), format.format(crs).getBytes(UTF_8));
            }
            catch (Exception e) {
                skippedCrs++;
            }
        }

        Map<Integer, OperationEntry> operationWkts = new TreeMap<>();
        Map<Pair, List<Integer>> operationCodesByPair = new TreeMap<>();
        int skippedOperations = 0;
        for (String code : operationFactory.getAuthorityCodes(CoordinateOperation.class)) {
            OptionalInt epsgCode = parseEpsgCode(code);
            if (epsgCode.isEmpty()) {
                skippedOperations++;
                continue;
            }
            try {
                CoordinateOperation operation = operationFactory.createCoordinateOperation(code);
                byte[] wkt = format.format(operation).getBytes(UTF_8);
                operationWkts.put(epsgCode.getAsInt(), new OperationEntry(epsgCode.getAsInt(), wkt));

                OptionalInt source = epsgIdentifier(operation.getSourceCRS());
                OptionalInt target = epsgIdentifier(operation.getTargetCRS());
                if (source.isPresent() && target.isPresent()) {
                    operationCodesByPair.computeIfAbsent(new Pair(source.getAsInt(), target.getAsInt()), _ -> new ArrayList<>())
                            .add(epsgCode.getAsInt());
                }
            }
            catch (Exception e) {
                skippedOperations++;
            }
        }

        Path catalog = outputDirectory.resolve(CATALOG_RESOURCE);
        writeCatalog(catalog, crsWkts, operationWkts, operationCodesByPair);
        copyResource("META-INF/LICENSE", resourceDirectory.resolve("EPSG-LICENSE.txt"));
        copyResource("META-INF/NOTICE", resourceDirectory.resolve("EPSG-NOTICE.txt"));

        System.out.printf(
                "Generated %s with %,d CRS definitions, %,d operations, %,d operation pairs; skipped %,d CRS and %,d operations%n",
                catalog,
                crsWkts.size(),
                operationWkts.size(),
                operationCodesByPair.size(),
                skippedCrs,
                skippedOperations);
        shutdownBuildTimeServices();
    }

    private static void writeCatalog(
            Path catalog,
            Map<Integer, byte[]> crsWkts,
            Map<Integer, OperationEntry> operationWkts,
            Map<Pair, List<Integer>> operationCodesByPair)
            throws IOException
    {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(catalog)))) {
            output.writeInt(0x54524745); // TRGE
            output.writeInt(FORMAT_VERSION);

            writeWktSection(output, crsWkts);
            writeWktSection(output, operationWkts.values().stream()
                    .collect(TreeMap::new, (map, operation) -> map.put(operation.code(), operation.wkt()), TreeMap::putAll));

            output.writeInt(operationCodesByPair.size());
            for (Map.Entry<Pair, List<Integer>> entry : operationCodesByPair.entrySet()) {
                output.writeInt(entry.getKey().source());
                output.writeInt(entry.getKey().target());
                List<Integer> operationCodes = entry.getValue().stream()
                        .sorted()
                        .toList();
                output.writeInt(operationCodes.size());
                for (int operationCode : operationCodes) {
                    output.writeInt(operationCode);
                }
            }
        }
    }

    private static void writeWktSection(DataOutputStream output, Map<Integer, byte[]> wkts)
            throws IOException
    {
        List<WktIndexEntry> entries = new ArrayList<>();
        List<CompressedBlock> blocks = new ArrayList<>();
        List<Map.Entry<Integer, byte[]>> records = List.copyOf(wkts.entrySet());
        ZstdCompressor compressor = ZstdCompressor.create();

        for (int blockRecordStart = 0; blockRecordStart < records.size(); blockRecordStart += WKT_BLOCK_RECORD_COUNT) {
            int blockRecordEnd = Math.min(blockRecordStart + WKT_BLOCK_RECORD_COUNT, records.size());
            ByteArrayOutputStream uncompressed = new ByteArrayOutputStream();
            int blockIndex = blocks.size();
            for (int recordIndex = blockRecordStart; recordIndex < blockRecordEnd; recordIndex++) {
                Map.Entry<Integer, byte[]> record = records.get(recordIndex);
                byte[] wkt = record.getValue();
                entries.add(new WktIndexEntry(record.getKey(), blockIndex, uncompressed.size(), wkt.length));
                uncompressed.write(wkt);
            }
            byte[] uncompressedBytes = uncompressed.toByteArray();
            byte[] compressedBytes = new byte[compressor.maxCompressedLength(uncompressedBytes.length)];
            int compressedLength = compressor.compress(uncompressedBytes, 0, uncompressedBytes.length, compressedBytes, 0, compressedBytes.length);
            byte[] block = new byte[compressedLength];
            arraycopy(compressedBytes, 0, block, 0, compressedLength);
            blocks.add(new CompressedBlock(uncompressedBytes.length, block));
        }

        output.writeInt(entries.size());
        for (WktIndexEntry entry : entries) {
            output.writeInt(entry.code());
            output.writeInt(entry.block());
            output.writeInt(entry.offset());
            output.writeInt(entry.length());
        }

        output.writeInt(blocks.size());
        for (CompressedBlock block : blocks) {
            output.writeInt(block.uncompressedLength());
            writeBytes(output, block.bytes());
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] bytes)
            throws IOException
    {
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void copyResource(String resource, Path target)
            throws IOException
    {
        try (InputStream input = GenerateEpsgCatalog.class.getClassLoader().getResourceAsStream(resource)) {
            if (input == null) {
                throw new IOException("Resource not found: " + resource);
            }
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static OptionalInt parseEpsgCode(String code)
    {
        requireNonNull(code, "code is null");
        int separator = code.lastIndexOf(':');
        if (separator >= 0) {
            code = code.substring(separator + 1);
        }
        try {
            return OptionalInt.of(Integer.parseInt(code));
        }
        catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }

    private static OptionalInt epsgIdentifier(IdentifiedObject object)
    {
        if (object == null) {
            return OptionalInt.empty();
        }
        for (Identifier identifier : object.getIdentifiers()) {
            if (!(identifier instanceof ReferenceIdentifier referenceIdentifier)) {
                continue;
            }
            String codeSpace = referenceIdentifier.getCodeSpace();
            if (codeSpace != null && codeSpace.equalsIgnoreCase("EPSG")) {
                return parseEpsgCode(identifier.getCode());
            }
        }
        return OptionalInt.empty();
    }

    private static void shutdownBuildTimeServices()
    {
        try {
            Class<?> shutdown = Class.forName("org.apache.sis.system.Shutdown");
            shutdown.getMethod("stop", Class.class).invoke(null, GenerateEpsgCatalog.class);
        }
        catch (ReflectiveOperationException | RuntimeException ignored) {
        }

        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true").close();
        }
        catch (SQLException ignored) {
            // Derby reports successful shutdown by throwing an SQLException.
        }
    }

    private record OperationEntry(int code, byte[] wkt) {}

    private record WktIndexEntry(int code, int block, int offset, int length) {}

    private record CompressedBlock(int uncompressedLength, byte[] bytes) {}

    private record Pair(int source, int target)
            implements Comparable<Pair>
    {
        @Override
        public int compareTo(Pair other)
        {
            return Comparator.comparingInt(Pair::source)
                    .thenComparingInt(Pair::target)
                    .compare(this, other);
        }
    }
}
