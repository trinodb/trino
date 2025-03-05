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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.trino.hive.formats.line.grok.exception.GrokException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

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
    private static final Gson PRETTY_GSON =
            new GsonBuilder().setPrettyPrinting().create();
    private static final Gson GSON = new GsonBuilder().create();
    private String subject; // texte
    private LinkedHashMap<String, Object> capture; // to maintain the order of fields
    private Garbage garbage;
    private Grok grok;
    private Matcher match;
    private int start;
    private int end;

    /**
     * For thread safety.
     */
    private static ThreadLocal<Match> matchHolder = new ThreadLocal<Match>()
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
        capture = new LinkedHashMap();
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
        return matchHolder.get();
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
                value = pairs.getValue().toString();

                KeyValue keyValue = Converter.convert(key, value, grok);

                // get validated key
                key = keyValue.getKey();

                // resolve value
                if (keyValue.getValue() instanceof String) {
                    value = cleanString((String) keyValue.getValue());
                }
                else {
                    value = keyValue.getValue();
                }

                // set if grok failure
                if (keyValue.hasGrokFailure()) {
                    capture.put(key + "_grokfailure", keyValue.getGrokFailure());
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
        Gson gs;
        if (pretty) {
            gs = PRETTY_GSON;
        }
        else {
            gs = GSON;
        }
        return gs.toJson(/* cleanMap( */capture/* ) */);
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
