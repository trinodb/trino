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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.line.grok.exception.GrokException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import static com.fasterxml.jackson.core.JsonFactory.Feature.INTERN_FIELD_NAMES;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;

/**
 * {@code Match} is a representation in {@code Grok} world of your log.
 *
 * @author anthonycorbacho
 * @since 0.0.1
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class Match
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(jsonFactoryBuilder().disable(INTERN_FIELD_NAMES).build());
    private String subject; // text
    private final LinkedHashMap<String, Object> capture; // to maintain the order of fields
    private final Garbage garbage;
    private Grok grok;
    private Matcher match;
    private int start;
    private int end;

    /**
     * For thread safety.
     */
    private static final ThreadLocal<Match> MATCH_HOLDER = new ThreadLocal<Match>()
    {
        @Override
        protected Match initialValue()
        {
            return new Match();
        }
    };

    /**
     * Create a new {@code Match} object.
     */
    public Match()
    {
        subject = "Nothing";
        grok = null;
        match = null;
        capture = new LinkedHashMap<>();
        garbage = new Garbage();
        start = 0;
        end = 0;
    }

    /**
     * Create Empty grok matcher.
     */
    public static final Match EMPTY = new Match();

    public void setGrok(Grok grok)
    {
        if (grok != null) {
            this.grok = grok;
        }
    }

    public Matcher getMatch()
    {
        return match;
    }

    public void setMatch(Matcher match)
    {
        this.match = match;
    }

    public int getStart()
    {
        return start;
    }

    public void setStart(int start)
    {
        this.start = start;
    }

    public int getEnd()
    {
        return end;
    }

    public void setEnd(int end)
    {
        this.end = end;
    }

    /**
     * Singleton.
     *
     * @return instance of Match
     */
    public static Match getInstance()
    {
        return MATCH_HOLDER.get();
    }

    /**
     * Set the single line of log to parse.
     *
     * @param text : single line of log
     */
    public void setSubject(String text)
    {
        if (text == null) {
            return;
        }
        if (text.isEmpty()) {
            return;
        }
        subject = text;
    }

    /**
     * Return the single line of log.
     *
     * @return the single line of log
     */
    public String getSubject()
    {
        return subject;
    }

    /**
     * Match to the <var>subject</var> the <var>regex</var> and save the matched element into a map.
     *
     */
    public void captures()
            throws GrokException
    {
        if (match == null) {
            throw new GrokException("Invalid log record!");
        }
        capture.clear();

        Map<String, String> mappedw = GrokUtils.namedGroups(this.match, this.subject);
        Iterator<Entry<String, String>> it = mappedw.entrySet().iterator();
        while (it.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map.Entry pairs = (Map.Entry) it.next();
            String key = null;
            Object value = null;
            if (this.grok.getNamedRegexCollectionById(pairs.getKey().toString()) == null) {
                key = pairs.getKey().toString();
            }
            else if (!this.grok.getNamedRegexCollectionById(pairs.getKey().toString()).isEmpty()) {
                key = this.grok.getNamedRegexCollectionById(pairs.getKey().toString());
            }
            if (pairs.getValue() != null) {
                value = pairs.getValue();

                ImmutableMap<String, Object> keyValue = ConverterFactory.convert(key, value, grok);
                key = keyValue.keySet().iterator().next();

                // resolve value
                if (keyValue.get(key) instanceof String) {
                    value = cleanString((String) keyValue.get(key));
                }
                else {
                    value = keyValue.get(key);
                }
            }
            if (value != null) {
                capture.put(key, value);
            }
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    /**
     * remove from the string the quote and double quote.
     *
     * @param value string to pure: "my/text"
     * @return unquoted string: my/text
     */
    private String cleanString(String value)
    {
        if (value == null) {
            return null;
        }
        if (value.isEmpty()) {
            return value;
        }
        char[] tmp = value.toCharArray();
        if (tmp.length == 1 && (tmp[0] == '"' || tmp[0] == '\'')) {
            value = ""; //empty string
        }
        else if ((tmp[0] == '"' && tmp[value.length() - 1] == '"')
                || (tmp[0] == '\'' && tmp[value.length() - 1] == '\'')) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    /**
     * Get the json representation of the matched element.
     * <p>
     * example: map [ {IP: 127.0.0.1}, {status:200}] will return {"IP":"127.0.0.1", "status":200}
     * </p>
     * If pretty is set to true, json will return prettyprint json string.
     *
     * @return Json of the matched element in the text
     */
    public String toJson(Boolean pretty)
    {
        if (capture == null) {
            return "{}";
        }
        if (capture.isEmpty()) {
            return "{}";
        }

        this.cleanMap();

        try {
            if (pretty) {
                return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(capture);
            }
            return OBJECT_MAPPER.writeValueAsString(capture);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the json representation of the matched element.
     * <p>
     * example: map [ {IP: 127.0.0.1}, {status:200}] will return {"IP":"127.0.0.1", "status":200}
     * </p>
     *
     * @return Json of the matched element in the text
     */
    public String toJson()
    {
        return toJson(false);
    }

    /**
     * Get the map representation of the matched element in the text.
     *
     * @return map object from the matched element in the text
     */
    public Map<String, Object> toMap()
    {
        this.cleanMap();
        return capture;
    }

    /**
     * Remove and rename the unwanted elements in the matched map.
     */
    private void cleanMap()
    {
        garbage.rename(capture);
        garbage.remove(capture);
    }

    /**
     * Util fct.
     *
     * @return boolean
     */
    public Boolean isNull()
    {
        return this.match == null;
    }
}
