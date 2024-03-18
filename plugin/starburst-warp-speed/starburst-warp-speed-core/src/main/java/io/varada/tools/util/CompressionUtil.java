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
package io.varada.tools.util;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil
{
    private CompressionUtil()
    {}

    public static byte[] compressGzip(String str)
            throws IOException
    {
        try (ByteArrayOutputStream obj = new ByteArrayOutputStream()) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(obj)) {
                gzip.write(str.getBytes(StandardCharsets.UTF_8));
                gzip.close();
                return obj.toByteArray();
            }
        }
    }

    public static String decompressGzip(byte[] bytes)
            throws IOException
    {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            BufferedReader bf = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
            StringBuilder outStr = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null) {
                outStr.append(line);
            }
            return outStr.toString();
        }
    }

    public static Object readCompress(byte[] bytes)
            throws IOException, ClassNotFoundException
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            try (GZIPInputStream gzip = new GZIPInputStream(bais)) {
                try (ObjectInputStream obj = new ObjectInputStream(gzip)) {
                    Object ret = obj.readObject();
                    obj.close();
                    return ret;
                }
            }
        }
    }

    public static byte[] compress(byte[] bytes)
            throws IOException
    {
        try (ByteArrayOutputStream obj = new ByteArrayOutputStream()) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(obj)) {
                gzip.write(bytes);
                gzip.close();
                return obj.toByteArray();
            }
        }
    }

    public static byte[] decompress(byte[] bytes)
            throws IOException
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            try (GZIPInputStream gis = new GZIPInputStream(bais)) {
                return IOUtils.toByteArray(gis);
            }
        }
    }
}
