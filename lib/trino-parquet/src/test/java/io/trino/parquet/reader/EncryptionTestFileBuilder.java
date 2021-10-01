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
package io.trino.parquet.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class EncryptionTestFileBuilder
{
    private MessageType schema;
    private Configuration conf;
    private Map<String, String> extraMeta = new HashMap<>();
    private int numRecord = 100000;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
    private int pageSize = ParquetProperties.DEFAULT_PAGE_SIZE;
    private String codec = "ZSTD";
    private String[] encryptColumns = {"DocId", "Name", "Links.Forward"};
    private ParquetCipher cipher = ParquetCipher.AES_GCM_V1;
    private Boolean footerEncryption = false;

    public EncryptionTestFileBuilder(Configuration conf)
    {
        this.conf = conf;
        schema = new MessageType("schema",
                new PrimitiveType(OPTIONAL, INT64, "DocId"),
                new PrimitiveType(REQUIRED, BINARY, "Name"),
                new PrimitiveType(OPTIONAL, BINARY, "Gender"),
                new GroupType(OPTIONAL, "Links",
                        new PrimitiveType(REPEATED, BINARY, "Backward"),
                        new PrimitiveType(REPEATED, BINARY, "Forward")));
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    }

    public EncryptionTestFileBuilder withNumRecord(int numRecord)
    {
        this.numRecord = numRecord;
        return this;
    }

    public EncryptionTestFileBuilder withExtraMeta(Map<String, String> extraMeta)
    {
        this.extraMeta = extraMeta;
        return this;
    }

    public EncryptionTestFileBuilder withWriterVersion(ParquetProperties.WriterVersion writerVersion)
    {
        this.writerVersion = writerVersion;
        return this;
    }

    public EncryptionTestFileBuilder withPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public EncryptionTestFileBuilder withCodec(String codec)
    {
        this.codec = codec;
        return this;
    }

    public EncryptionTestFileBuilder withEncryptColumns(String[] encryptColumns)
    {
        this.encryptColumns = encryptColumns;
        return this;
    }

    public EncryptionTestFileBuilder withFooterEncryption()
    {
        this.footerEncryption = true;
        return this;
    }

    public String build()
            throws IOException
    {
        String file = createTempFile("test");
        TestDocs testDocs = new TestDocs(numRecord);
        FileEncryptionProperties encrProperties = EncDecPropertiesHelper.getFileEncryptionProperties(encryptColumns, cipher, footerEncryption);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file))
                .withConf(conf)
                .withWriterVersion(writerVersion)
                .withExtraMetaData(extraMeta)
                .withValidation(true)
                .withPageSize(pageSize)
                .withEncryption(encrProperties)
                .withCompressionCodec(CompressionCodecName.valueOf(codec));
        try (ParquetWriter writer = builder.build()) {
            for (int i = 0; i < numRecord; i++) {
                SimpleGroup g = new SimpleGroup(schema);
                g.add("DocId", testDocs.docId[i]);
                g.add("Name", testDocs.name[i]);
                g.add("Gender", testDocs.gender[i]);
                Group links = g.addGroup("Links");
                links.add(0, testDocs.linkBackward[i]);
                links.add(1, testDocs.linkForward[i]);
                writer.write(g);
            }
        }
        return file;
    }

    private static long getLong()
    {
        return ThreadLocalRandom.current().nextLong(1000);
    }

    private static String getString()
    {
        Random rnd = new Random(5);
        char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append(chars[rnd.nextInt(10)]);
        }
        return sb.toString();
    }

    public static String createTempFile(String prefix)
    {
        try {
            return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
        }
        catch (IOException e) {
            throw new AssertionError("Unable to create temporary file", e);
        }
    }

    public static class TestDocs
    {
        public long[] docId;
        public String[] name;
        public String[] gender;
        public String[] linkBackward;
        public String[] linkForward;

        public TestDocs(int numRecord)
        {
            docId = new long[numRecord];
            for (int i = 0; i < numRecord; i++) {
                docId[i] = getLong();
            }

            name = new String[numRecord];
            for (int i = 0; i < numRecord; i++) {
                name[i] = getString();
            }

            gender = new String[numRecord];
            for (int i = 0; i < numRecord; i++) {
                gender[i] = getString();
            }

            linkBackward = new String[numRecord];
            for (int i = 0; i < numRecord; i++) {
                linkBackward[i] = getString();
            }

            linkForward = new String[numRecord];
            for (int i = 0; i < numRecord; i++) {
                linkForward[i] = getString();
            }
        }
    }
}
