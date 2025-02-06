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
class DateConverter
        extends IConverter<Date>
{
    @Override
    public Date convert(String value)
            throws Exception
    {
        return DateFormat.getDateTimeInstance(DateFormat.SHORT,
                DateFormat.SHORT,
                Converter.locale).parse(value);
    }

    @Override
    public Date convert(String value, String informat)
            throws Exception
    {
        SimpleDateFormat formatter = new SimpleDateFormat(informat, Converter.locale);
        return formatter.parse(value);
    }
}
