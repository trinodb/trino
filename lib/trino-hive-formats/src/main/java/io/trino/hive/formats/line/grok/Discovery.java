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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code Discovery} try to find the best pattern for the given string.
 *
 * @author anthonycorbacho
 * @since 0.0.2
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class Discovery
{
    private Grok grok;

    /**
     * Create a new {@code Discovery} object.
     *
     * @param grok instance of grok
     */
    public Discovery(Grok grok)
    {
        this.grok = grok;
    }

    /**
     * Sort by regex complexity.
     *
     * @param groks Map of the pattern name and grok instance
     * @return the map sorted by grok pattern complexity
     */
    private Map<String, Grok> sort(Map<String, Grok> groks)
    {
        List<Grok> groky = new ArrayList<Grok>(groks.values());
        Map<String, Grok> mGrok = new LinkedHashMap<String, Grok>();
        Collections.sort(groky, new Comparator<Grok>() {
            @Override
            public int compare(Grok g1, Grok g2)
            {
                return (this.complexity(g1.getNamedRegex()) < this.complexity(g2.getNamedRegex())) ? 1
                        : 0;
            }

            private int complexity(String expandedPattern)
            {
                int score = 0;
                score += expandedPattern.split("\\Q" + "|" + "\\E", -1).length - 1;
                score += expandedPattern.length();
                return score;
            }
        });

        for (Grok g : groky) {
            mGrok.put(g.getSaved_pattern(), g);
        }
        return mGrok;
    }

    /**
     *
     * @param expandedPattern regex string
     * @return the complexity of the regex
     */
    private int complexity(String expandedPattern)
    {
        int score = 0;

        score += expandedPattern.split("\\Q" + "|" + "\\E", -1).length - 1;
        score += expandedPattern.length();

        return score;
    }

    /**
     * Find a pattern from a log.
     *
     * @param text witch is the representation of your single
     * @return Grok pattern %{Foo}...
     */
    public String discover(String text)
            throws GrokException
    {
        if (text == null) {
            return "";
        }

        Map<String, Grok> groks = new TreeMap<String, Grok>();
        Map<String, String> gPatterns = grok.getPatterns();
        String texte = text;

        // Compile the pattern
        Iterator<Entry<String, String>> it = gPatterns.entrySet().iterator();
        while (it.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map.Entry pairs = (Map.Entry) it.next();
            String key = pairs.getKey().toString();
            Grok g = new Grok();

            try {
                g.copyPatterns(gPatterns);
                g.setSaved_pattern(key);
                g.compile("%{" + key + "}");
                groks.put(key, g);
            }
            catch (GrokException e) {
                // TODO: Add logger
                continue;
            }
        }

        // Sort patterns by complexity
        Map<String, Grok> patterns = this.sort(groks);

        Iterator<Entry<String, Grok>> pit = patterns.entrySet().iterator();
        while (pit.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map.Entry pairs = (Map.Entry) pit.next();
            String key = pairs.getKey().toString();
            Grok value = (Grok) pairs.getValue();

            // We want to search with more complex pattern
            // We avoid word, small number, space....
            if (this.complexity(value.getNamedRegex()) < 20) {
                continue;
            }

            Match m = value.match(text);
            if (m.isNull()) {
                continue;
            }
            // get the part of the matched text
            String part = getPart(m, text);

            // we skip boundary word
            Pattern pattern = Pattern.compile(".\\b.");
            Matcher ma = pattern.matcher(part);
            if (!ma.find()) {
                continue;
            }

            // We skip the part that already include %{Foo}
            Pattern pattern2 = Pattern.compile("%\\{[^}+]\\}");
            Matcher ma2 = pattern2.matcher(part);

            if (ma2.find()) {
                continue;
            }
            texte = StringUtils.replace(texte, part, "%{" + key + "}");
        }

        return texte;
    }

    /**
     * Get the substring that match with the text.
     *
     * @param m Grok Match
     * @param text text
     * @return string
     */
    private String getPart(Match m, String text)
    {
        if (m == null || text == null) {
            return "";
        }

        return text.substring(m.getStart(), m.getEnd());
    }
}
