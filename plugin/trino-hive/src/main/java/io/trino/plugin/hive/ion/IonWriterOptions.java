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
package io.trino.plugin.hive.ion;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import io.trino.spi.TrinoException;

import java.io.OutputStream;
import java.util.Locale;
import java.util.Map;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;

public final class IonWriterOptions
{
    public static final String ION_ENCODING_PROPERTY = "ion.encoding";
    public static final String TEXT_ENCODING = "text";
    public static final String BINARY_ENCODING = "binary";

    public enum IonEncoding
    {
        BINARY
        {
            @Override
            public IonWriter createWriter(OutputStream outputStream)
            {
                return IonBinaryWriterBuilder.standard().build(outputStream);
            }
        },

        TEXT
        {
            @Override
            public IonWriter createWriter(OutputStream outputStream)
            {
                return IonTextWriterBuilder.minimal().build(outputStream);
            }
        };

        public abstract IonWriter createWriter(OutputStream outputStream);
    }

    public static IonEncoding getIonEncoding(Map<String, String> schema)
    {
        String encodingStr = schema.getOrDefault(ION_ENCODING_PROPERTY, BINARY_ENCODING);
        return switch (encodingStr.toLowerCase(Locale.ROOT)) {
            case TEXT_ENCODING -> IonEncoding.TEXT;
            case BINARY_ENCODING -> IonEncoding.BINARY;
            default -> throw new TrinoException(HIVE_UNSUPPORTED_FORMAT,
                    "Unsupported Ion encoding format: " + encodingStr);
        };
    }

    private IonWriterOptions() {}
}
