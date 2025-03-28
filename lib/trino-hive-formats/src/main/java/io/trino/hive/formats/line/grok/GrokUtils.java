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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code GrokUtils} contain set of useful tools or methods.
 *
 * @author anthonycorbacho
 * @since 0.0.6
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class GrokUtils
{
    private GrokUtils() {}

    /**
     * Extract Grok patter like %{FOO} to FOO, Also Grok pattern with semantic.
     */

    public static final Pattern GROK_PATTERN = Pattern.compile(
            "%\\{" +
                    "(?<name>" +
                    "(?<pattern>[A-z0-9]+)" +
                    "(?::(?<subname>[A-z0-9_:;\\/\\s\\.]+))?" +
                    ")" +
                    "(?:=(?<definition>" +
                    "(?:" +
                    "(?:[^{}]+|\\.+)+" +
                    ")+" +
                    ")" +
                    ")?" +
                    "\\}");

    public static final Pattern NAMED_REGEX = Pattern
            .compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

    private static Set<String> getNameGroups(String regex)
    {
        Set<String> namedGroups = new LinkedHashSet<String>();
        Matcher m = NAMED_REGEX.matcher(regex);
        while (m.find()) {
            namedGroups.add(m.group(1));
        }
        return namedGroups;
    }

    public static Map<String, String> namedGroups(Matcher matcher,
                                                  String namedRegex)
    {
        Set<String> groupNames = getNameGroups(matcher.pattern().pattern());
        Matcher localMatcher = matcher.pattern().matcher(namedRegex);
        Map<String, String> namedGroups = new LinkedHashMap<String, String>();
        if (localMatcher.find()) {
            for (String groupName : groupNames) {
                String groupValue = localMatcher.group(groupName);
                namedGroups.put(groupName, groupValue);
            }
        }
        return namedGroups;
    }
}
