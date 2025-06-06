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

import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.line.grok.Converter.BooleanConverter;
import io.trino.hive.formats.line.grok.Converter.ByteConverter;
import io.trino.hive.formats.line.grok.Converter.DateConverter;
import io.trino.hive.formats.line.grok.Converter.DoubleConverter;
import io.trino.hive.formats.line.grok.Converter.FloatConverter;
import io.trino.hive.formats.line.grok.Converter.IntegerConverter;
import io.trino.hive.formats.line.grok.Converter.LongConverter;
import io.trino.hive.formats.line.grok.Converter.ShortConverter;
import io.trino.hive.formats.line.grok.Converter.StringConverter;
import io.trino.hive.formats.line.grok.exception.GrokException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
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
public class ConverterFactory
{
    private ConverterFactory() {}

    public static final Map<String, Converter<?>> CONVERTERS;
    public static final Locale LOCALE = Locale.ENGLISH;
    private static final int MAX_SPEC_PARTS = 3; // field ID, datatype, and datetype arguments
    private static final int FIELD_ID_AND_DATATYPE = 2;
    private static final int ONLY_FIELD_ID = 1;
    private static final int FIELD_ID_IDX = 0;
    private static final int DATATYPE_IDX = 1;
    private static final int DATATYPE_ARGS_IDX = 2;

    static {
        CONVERTERS = ImmutableMap.<String, Converter<?>>builder()
                .put("byte", new ByteConverter())
                .put("boolean", new BooleanConverter())
                .put("short", new ShortConverter())
                .put("int", new IntegerConverter())
                .put("long", new LongConverter())
                .put("float", new FloatConverter())
                .put("double", new DoubleConverter())
                .put("date", new DateConverter())
                .put("datetime", new DateConverter())
                .put("string", new StringConverter())
                .buildOrThrow();
    }

    private static Converter<?> getConverter(String key)
            throws Exception
    {
        Converter<?> converter = CONVERTERS.get(key);
        if (converter == null) {
            throw new Exception("Invalid data type :" + key);
        }
        return converter;
    }

    /**
     * Convert a value according to the specified key pattern and Grok config
     *
     * The key can be of the form:
     * fieldID
     * fieldID:datatype
     * fieldID:datatype:datatypeArgs
     *
     * fieldID - Identifier of field being parsed
     * datatype - (Optional) target data type (e.g. int, string, date)
     * args - (Optional) arguments to the data type (e.g. date format)
     *
     * @param key The pattern key with components field, data type, and args
     * @param value The value to convert
     * @param grok Grok instance containing pattern configs and conversion settings (e.g. strict mode)
     * @return ImmutableMap containing the field ID and its converted value
     * @throws GrokException If conversion fails or if pattern/datatype is invalid
     *
     * converting a timestamp: convert("timestamp:date:yyyy-MM-dd", "2023-12-25", grok)
     * timestamp is the field ID, date is the data type, and yyyy-MM-dd is the date format argument
     *
     * converting int: convert("status:int", "200", grok)
     * status is the field ID, int is the data type
     *
     * using default data type from pattern: convert("message", "Hello World", grok)
     * message is the field ID, no data type is specified, so the default data type from the pattern is used
     *
     */
    public static ImmutableMap<String, Object> convert(String key, Object value, Grok grok)
            throws GrokException
    {
        String[] spec = key.split(";|:", MAX_SPEC_PARTS);
        try {
            // process situations with field id [and datatype]
            if (spec.length <= FIELD_ID_AND_DATATYPE) {
                String pattern = grok.getGrokPatternPatterns().get(key); // actual pattern name
                String defaultDataType = grok.getGrokPatternDefaultDatatype().get(pattern); // default datatype of the pattern
                // process Date datatype with no format arguments
                // 1. not in strict mode && no assigned data type && the default data type is datetime or date
                // 2. assigned data type is datetime or date && no date format argument
                if ((!grok.getStrictMode() && spec.length == ONLY_FIELD_ID && defaultDataType != null && (defaultDataType.equals("datetime") || defaultDataType.equals("date")))
                        || (spec.length == FIELD_ID_AND_DATATYPE && (spec[DATATYPE_IDX].equals("datetime") || spec[DATATYPE_IDX].equals("date")))) {
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
                        return ImmutableMap.of(spec[FIELD_ID_IDX], date);
                    }
                    else {
                        // if failed, return string object
                        return ImmutableMap.of(spec[FIELD_ID_IDX], String.valueOf(value));
                    }
                }
                else if (spec.length == ONLY_FIELD_ID) {
                    if (grok.getStrictMode()) {
                        // if in strict mode, never do automatic data type conversion
                        defaultDataType = null;
                    }
                    // process situations with only field id (check default datatype, except date and datetime)
                    return ImmutableMap.of(spec[FIELD_ID_IDX],
                            defaultDataType == null ? String.valueOf(value) : getConverter(defaultDataType).convert(String.valueOf(value)));
                }
                else {
                    // process situations with field id and datatype (except date and datetime)
                    return ImmutableMap.of(spec[FIELD_ID_IDX], getConverter(spec[DATATYPE_IDX]).convert(String.valueOf(value)));
                }
            }
            else if (spec.length == MAX_SPEC_PARTS) {
                // process situations with field id, datatype and datatype arguments
                return ImmutableMap.of(spec[FIELD_ID_IDX], getConverter(spec[DATATYPE_IDX]).convert(String.valueOf(value), spec[DATATYPE_ARGS_IDX]));
            }
            else {
                throw new GrokException("Unsupported spec : " + key);
            }
        }
        catch (Exception e) {
            if (!grok.getStrictMode()) {
                // if not in strict mode, try to convert everything to string when meeting a data type conversion error
                return ImmutableMap.of(spec[0], String.valueOf(value));
            }
            else {
                // if in strict mode, throw exception when meeting a data type conversion error
                throw new GrokException("Unable to finish data type conversion of " + spec[FIELD_ID_IDX] + ":" + e.getMessage());
            }
        }
    }
}
