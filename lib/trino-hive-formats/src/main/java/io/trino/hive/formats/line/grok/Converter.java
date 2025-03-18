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
package io.trino.hive.formats.line.grok;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public sealed interface Converter<T>
{
    default T convert(String value, String format)
            throws Exception
    {
        return null;
    }

    T convert(String value)
            throws Exception;

    final class BooleanConverter
            implements Converter<Boolean>
    {
        @Override
        public Boolean convert(String value)
                throws Exception
        {
            return Boolean.parseBoolean(value);
        }
    }

    final class ByteConverter
            implements Converter<Byte>
    {
        @Override
        public Byte convert(String value)
                throws Exception
        {
            return Byte.parseByte(value);
        }
    }

    final class DateConverter
            implements Converter<Date>
    {
        @Override
        public Date convert(String value)
                throws Exception
        {
            return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, ConverterFactory.LOCALE).parse(value);
        }

        @Override
        public Date convert(String value, String informat)
                throws Exception
        {
            SimpleDateFormat formatter = new SimpleDateFormat(informat, ConverterFactory.LOCALE);
            return formatter.parse(value);
        }
    }

    final class DoubleConverter
            implements Converter<Double>
    {
        @Override
        public Double convert(String value)
                throws Exception
        {
            return Double.parseDouble(value);
        }
    }

    final class FloatConverter
            implements Converter<Float>
    {
        @Override
        public Float convert(String value)
                throws Exception
        {
            return Float.parseFloat(value);
        }
    }

    final class IntegerConverter
            implements Converter<Integer>
    {
        @Override
        public Integer convert(String value)
                throws Exception
        {
            return Integer.parseInt(value);
        }
    }

    final class LongConverter
            implements Converter<Long>
    {
        @Override
        public Long convert(String value)
                throws Exception
        {
            return Long.parseLong(value);
        }
    }

    final class ShortConverter
            implements Converter<Short>
    {
        @Override
        public Short convert(String value)
                throws Exception
        {
            return Short.parseShort(value);
        }
    }

    final class StringConverter
            implements Converter<String>
    {
        @Override
        public String convert(String value)
                throws Exception
        {
            return value;
        }
    }
}
