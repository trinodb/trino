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

import io.trino.hive.formats.line.grok.exception.GrokException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Convert String argument to the right type.
 *
 * @author anthonyc
 *
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class Converter
{
    private Converter() {}

    public static Map<String, IConverter<?>> converters = new HashMap<String, IConverter<?>>();
    public static Locale locale = Locale.ENGLISH;

    static {
        converters.put("byte", new ByteConverter());
        converters.put("boolean", new BooleanConverter());
        converters.put("short", new ShortConverter());
        converters.put("int", new IntegerConverter());
        converters.put("long", new LongConverter());
        converters.put("float", new FloatConverter());
        converters.put("double", new DoubleConverter());
        converters.put("date", new DateConverter());
        converters.put("datetime", new DateConverter());
        converters.put("string", new StringConverter());
    }

    private static IConverter getConverter(String key)
            throws Exception
    {
        IConverter converter = converters.get(key);
        if (converter == null) {
            throw new Exception("Invalid data type :" + key);
        }
        return converter;
    }

    public static KeyValue convert(String key, Object value, Grok grok)
            throws GrokException
    {
        String[] spec = key.split(";|:", 3);
        try {
            // process situations with fieldid [and datatype]
            if (spec.length <= 2) {
                String pattern = grok.getGrokPatternPatterns().get(key); // actual pattern name
                String defaultDataType = grok.getGrokPatternDefaultDatatype().get(pattern); // default datatype of the pattern
                // process Date datatype with no format arguments
                // 1. not in strict mode && no assigned data type && the default data type is datetime or date
                // 2. assigned data type is datetime or date && no date format argument
                if ((!grok.getStrictMode() && spec.length == 1 && defaultDataType != null && (defaultDataType.equals("datetime") || defaultDataType.equals("date")))
                        || (spec.length == 2 && (spec[1].equals("datetime") || spec[1].equals("date")))) {
                    // check whether to get the date format already when parsing the previous records
                    String dateFormat = grok.getGrokPatternPatterns().get(key + "dateformat");
                    Date date = null;
                    if (dateFormat != null) {
                        //if yes, use that format
                        date = (Date) getConverter("datetime").convert(String.valueOf(value), dateFormat);
                    }
                    else {
                        // if no, infer the date format, and save it
                        ArrayList<String> currDateFormats = grok.getGrokDateFormats().get(pattern);
                        if (currDateFormats != null) {
                            for (String format : currDateFormats) {
                                try {
                                    date = (Date) getConverter("datetime").convert(String.valueOf(value), format);
                                    grok.getGrokPatternPatterns().put(key + "dateformat", format);
                                    break;
                                }
                                catch (ParseException pe) {
                                    // only continue on a conversion exception
                                    continue;
                                }
                            }
                        }
                    }
                    if (date != null) {
                        // if parse successfully, return date object
                        return new KeyValue(spec[0], date);
                    }
                    else {
                        // if failed, return string object
                        return new KeyValue(spec[0], String.valueOf(value));
                    }
                }
                else if (spec.length == 1) {
                    if (grok.getStrictMode()) {
                        // if in strict mode, never do automatic data type conversion
                        defaultDataType = null;
                    }
                    // process situations with only fieldid (check default datatype, except date and datetime)
                    return new KeyValue(spec[0],
                            defaultDataType == null ? String.valueOf(value) : getConverter(defaultDataType).convert(String.valueOf(value)));
                }
                else {
                    // process situations with fieldid and datatype (except date and datetime)
                    return new KeyValue(spec[0], getConverter(spec[1]).convert(String.valueOf(value)));
                }
            }
            else if (spec.length == 3) {
                // process situations with fieldid, datatype and datatype arguments
                return new KeyValue(spec[0], getConverter(spec[1]).convert(String.valueOf(value), spec[2]));
            }
            else {
                throw new GrokException("Unsupported spec : " + key);
            }
        }
        catch (Exception e) {
            if (!grok.getStrictMode()) {
                // if not in strict mode, try to convert everything to string when meeting a data type conversion error
                return new KeyValue(spec[0], String.valueOf(value));
            }
            else {
                // if in strict mode, throw exception when meeting a data type conversion error
                throw new GrokException("Unable to finish data type conversion of " + spec[0] + ":" + e.getMessage());
            }
        }
    }
}
