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
package io.trino.orc;

import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.MetadataReader;
import io.trino.orc.metadata.PostScript;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.trino.orc.OrcReader.validateWrite;
import static io.trino.orc.metadata.PostScript.MAGIC;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class OrcFileTailReader
{
    private static final Logger log = Logger.get(OrcFileTailReader.class);

    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private OrcFileTailReader() {}

    public static Optional<OrcFileTail> getOrcFileTail(OrcDataSource orcDataSource, MetadataReader metadataReader, Optional<OrcWriteValidation> writeValidation)
            throws IOException
    {
        // read the tail of the file, and check if the file is actually empty
        long estimatedFileSize = orcDataSource.getEstimatedSize();
        if (estimatedFileSize > 0 && estimatedFileSize <= MAGIC.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid file size %s", estimatedFileSize);
        }

        long expectedReadSize = min(estimatedFileSize, EXPECTED_FOOTER_SIZE);
        Slice fileTail = orcDataSource.readTail(toIntExact(expectedReadSize));
        if (fileTail.length() == 0) {
            return Optional.empty();
        }

        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 1 byte: postScriptSize

        // get length of PostScript - last byte of the file
        int postScriptSize = fileTail.getUnsignedByte(fileTail.length() - SIZE_OF_BYTE);
        if (postScriptSize >= fileTail.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid postscript length %s", postScriptSize);
        }

        // decode the post script
        PostScript postScript;
        try {
            postScript = metadataReader.readPostScript(fileTail.slice(fileTail.length() - SIZE_OF_BYTE - postScriptSize, postScriptSize).getInput());
        }
        catch (OrcCorruptionException e) {
            // check if this is an ORC file and not an RCFile or something else
            try {
                Slice headerMagic = orcDataSource.readFully(0, MAGIC.length());
                if (!MAGIC.equals(headerMagic)) {
                    throw new OrcCorruptionException(orcDataSource.getId(), "Not an ORC file");
                }
            }
            catch (IOException ignored) {
                // throw original exception
            }

            throw e;
        }

        // verify this is a supported version
        checkOrcVersion(orcDataSource, postScript.getVersion());
        validateWrite(writeValidation, orcDataSource, validation -> validation.getVersion().equals(postScript.getVersion()), "Unexpected version");

        int bufferSize = toIntExact(postScript.getCompressionBlockSize());

        // check compression codec is supported
        CompressionKind compressionKind = postScript.getCompression();
        validateWrite(writeValidation, orcDataSource, validation -> validation.getCompression() == compressionKind, "Unexpected compression");

        PostScript.HiveWriterVersion hiveWriterVersion = postScript.getHiveWriterVersion();

        int footerSize = toIntExact(postScript.getFooterLength());
        int metadataSize = toIntExact(postScript.getMetadataLength());

        // check if extra bytes need to be read
        Slice completeFooterSlice;
        int completeFooterSize = footerSize + metadataSize + postScriptSize + SIZE_OF_BYTE;
        if (completeFooterSize > fileTail.length()) {
            // initial read was not large enough, so just read again with the correct size
            completeFooterSlice = orcDataSource.readTail(completeFooterSize);
        }
        else {
            // footer is already in the bytes in fileTail, just adjust position, length
            completeFooterSlice = fileTail.slice(fileTail.length() - completeFooterSize, completeFooterSize);
        }

        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);

        return Optional.of(
                new OrcFileTail(
                        hiveWriterVersion,
                        bufferSize,
                        compressionKind,
                        footerSlice,
                        footerSize,
                        metadataSlice,
                        metadataSize));
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    // This is based on the Apache Hive ORC code
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file %s was written by a newer Hive version %s. This file may not be readable by this version of Hive (%s.%s).",
                        orcDataSource,
                        Joiner.on('.').join(version),
                        CURRENT_MAJOR_VERSION,
                        CURRENT_MINOR_VERSION);
            }
        }
    }
}
