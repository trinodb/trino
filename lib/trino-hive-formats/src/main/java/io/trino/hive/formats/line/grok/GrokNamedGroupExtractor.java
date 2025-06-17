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
import java.util.stream.Collectors;

/**
 * {@code GrokUtils} contain set of useful tools or methods.
 *
 * @author anthonycorbacho
 * @since 0.0.6
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class GrokNamedGroupExtractor
{
    private GrokNamedGroupExtractor()
    {
    }

    /**
     * Extract Grok pattern like %{FOO} to FOO, Also Grok pattern with semantic.
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

    public static Map<String, String> namedGroups(Matcher matcher, Boolean isCompiled)
    {
        if (isCompiled) {
            Set<String> sortedKeys = matcher.namedGroups().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return namedGroupsMatcher(matcher, sortedKeys);
        }
        else {
            return namedGroupsMatcher(matcher, matcher.namedGroups().keySet());
        }
    }

    private static Map<String, String> namedGroupsMatcher(Matcher matcher, Set<String> groupNames)
    {
        Map<String, String> namedGroups = new LinkedHashMap<>();
        if (matcher.find(0)) {
            for (String groupName : groupNames) {
                String groupValue = matcher.group(groupName);
                namedGroups.put(groupName, groupValue);
            }
        }
        return namedGroups;
    }
}
