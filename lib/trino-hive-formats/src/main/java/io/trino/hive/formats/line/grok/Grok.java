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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import io.trino.hive.formats.line.grok.exception.GrokException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.hive.formats.line.grok.GrokNamedGroupExtractor.GROK_PATTERN_NAMED_GROUPS;
import static java.util.Objects.requireNonNull;

/**
 * {@code Grok} parse arbitrary text and structure it.<br>
 *
 * {@code Grok} is simple API that allows you to easily parse logs
 * and other files (single line). With {@code Grok},
 * you can turn unstructured log and event data into structured data (JSON).
 *<br>
 * example:<br>
 * <pre>
 *  Grok grok = Grok.create("patterns/patterns");
 *  grok.compile("%{USER}");
 *  Match gm = grok.match("root");
 *  gm.captures();
 * </pre>
 *
 * @since 0.0.1
 * @author anthonycorbacho
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class Grok
        implements Serializable
{
    /**
     * Named regex of the originalGrokPattern.
     */
    private String namedRegex;
    /**
     * Map of the named regex of the originalGrokPattern
     * with id = namedregexid and value = namedregex.
     */
    private final Map<String, String> namedRegexCollection;
    /**
     * Original {@code Grok} pattern (expl: %{IP}).
     */
    private String originalGrokPattern;
    /**
     * Pattern of the namedRegex.
     */
    private Pattern compiledNamedRegex;

    public Set<String> namedGroups;

    /**
     * {@code Grok} patterns definition.
     */
    private Map<String, String> grokPatternDefinition;

    /**
     * {@code Grok} default patterns data type.
     */
    private Map<String, String> grokPatternDefaultDatatype;

    /**
     * {@code Grok} actual patterns of each field.
     */
    private final Map<String, String> grokPatternPatterns;

    /**
     * {@code Grok} date formats.
     */
    private Map<String, ArrayList<String>> grokDateFormats;

    private static final Pattern DEFAULT_PATTERN = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");
    private static final Pattern RENAMED_CAPTURE_GROUP_PATTERN = Pattern.compile("\\(\\?<([^>]+)>");

    private static final ImmutableSortedMap<String, String> DEFAULT_PATTERNS;
    private static final ImmutableMap<String, String> DEFAULT_DATA_TYPES;
    private static final ImmutableMap<String, ImmutableList<String>> DEFAULT_DATE_FORMATS;

    static {
        try {
            Map<String, String> patterns = new TreeMap<>();
            Map<String, String> dataTypes = new HashMap<>();
            Map<String, ImmutableList.Builder<String>> dateFormats = new HashMap<>();

            // Load default patterns statically without creating an instance
            loadDefaultPatternsFromReader(getFileFromResources("grok/patterns"), patterns);
            loadDefaultPatternsFromReader(getFileFromResources("grok/datatype"), dataTypes);
            loadDefaultDatePatternsFromReader(getFileFromResources("grok/dateformat"), dateFormats);

            DEFAULT_PATTERNS = ImmutableSortedMap.copyOf(patterns);
            DEFAULT_DATA_TYPES = ImmutableMap.copyOf(dataTypes);
            DEFAULT_DATE_FORMATS = ImmutableMap.copyOf(Maps.transformValues(dateFormats, ImmutableList.Builder::build));
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * used to decide grok mode:
     *    - true: strict mode, no automate data type conversion, throw exceptions when data type conversion fails
     *    - false: default mode, do automate data type conversion, always convert to string when data type conversion fails
     */
    private boolean strictMode;

    /**
     * Create a new <i>empty</i>{@code Grok} object.
     */
    public Grok()
    {
        originalGrokPattern = "";
        namedRegex = "";
        compiledNamedRegex = null;
        grokPatternDefinition = new TreeMap<>(DEFAULT_PATTERNS);
        grokPatternDefaultDatatype = new HashMap<>(DEFAULT_DATA_TYPES);
        grokPatternPatterns = new HashMap<>();
        grokDateFormats = copyDateFormats();
        namedRegexCollection = new TreeMap<>();
        strictMode = false;
    }

    private static Map<String, ArrayList<String>> copyDateFormats()
    {
        return new HashMap<>(Maps.transformValues(DEFAULT_DATE_FORMATS, formats -> new ArrayList<>(formats)));
    }

    /**
     * read config info from the given reader
     *
     * @param reader reader that contains config info
     * @param defaultConfig default value of config
     * @throws GrokException grok runtime exception
     */
    private static void loadDefaultPatternsFromReader(Reader reader, Map<String, String> defaultConfig)
            throws GrokException
    {
        try (BufferedReader br = new BufferedReader(reader)) {
            String line;
            while ((line = br.readLine()) != null) {
                // We dont want \n and commented line
                Matcher m = DEFAULT_PATTERN.matcher(line);
                if (m.matches()) {
                    loadDefaultPatterns(m.group(1), m.group(2), defaultConfig);
                }
            }
        }
        catch (IOException e) {
            throw new GrokException(e.getMessage());
        }
    }

    private static void loadDefaultDatePatternsFromReader(Reader reader, Map<String, ImmutableList.Builder<String>> defaultConfig)
            throws GrokException
    {
        try (BufferedReader br = new BufferedReader(reader)) {
            String line;
            String key = null;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) {
                    key = null;
                }
                else if (key == null) {
                    key = line;
                }
                else {
                    loadDefaultDatePatterns(key, line, defaultConfig);
                }
            }
        }
        catch (IOException e) {
            throw new GrokException(e.getMessage());
        }
    }

    /**
     * Load config files that holds default patterns
     *
     * @param name Pattern Name
     * @param conf Config value to be added
     * @param defaultConfig which config file is currently operated
     * @throws GrokException grok runtime exception
     */
    private static void loadDefaultPatterns(String name, String conf, Map<String, String> defaultConfig)
            throws GrokException
    {
        if (name == null || name.isBlank()) {
            throw new GrokException("Invalid pattern name when loading config file");
        }
        if (conf == null || conf.isBlank()) {
            throw new GrokException("Invalid value when loading config file");
        }
        defaultConfig.put(name, conf);
    }

    private static void loadDefaultDatePatterns(String name, String conf, Map<String, ImmutableList.Builder<String>> defaultConfig)
            throws GrokException
    {
        if (name == null || name.isBlank()) {
            throw new GrokException("Invalid pattern name when loading config file");
        }
        if (conf == null || conf.isBlank()) {
            throw new GrokException("Invalid value when loading config file");
        }

        if (defaultConfig.containsKey(name)) {
            defaultConfig.get(name).add(conf);
        }
        else {
            defaultConfig.put(name, ImmutableList.<String>builder().add(conf));
        }
    }

    /**
     * Read file from resources as stream reader
     *
     * @param filePath the file path in resources, e.g. patterns/patterns
     * @return the reader that contains specific file content
     * @throws GrokException runtime expt
     */
    static Reader getFileFromResources(String filePath)
    {
        return new InputStreamReader(requireNonNull(Grok.class.getClassLoader().getResourceAsStream(filePath)), StandardCharsets.UTF_8);
    }

    public void addPatternFromReader(Reader reader)
            throws GrokException
    {
        try (BufferedReader br = new BufferedReader(reader)) {
            String line;
            while ((line = br.readLine()) != null) {
                // We don't want \n and commented line
                Matcher m = DEFAULT_PATTERN.matcher(line);
                if (m.matches()) {
                    addPattern(m.group(1), m.group(2));
                }
            }
        }
        catch (IOException e) {
            throw new GrokException(e.getMessage());
        }
    }

    public void addPattern(String name, String conf)
            throws GrokException
    {
        if (name == null || name.isBlank()) {
            throw new GrokException("Invalid pattern name when loading config file");
        }
        if (conf == null || conf.isBlank()) {
            throw new GrokException("Invalid value when loading config file");
        }
        grokPatternDefinition.put(name, conf);
    }

    /**
     * Match the given <var>text</var> with the named regex
     * {@code Grok} will extract data from the string and get an extence of {@link Match}.
     *
     * @param text : Single line of log
     * @return Grok Match
     */
    public Match match(String text)
    {
        if (text == null || text.isBlank()) {
            return Match.EMPTY;
        }

        Matcher m = compiledNamedRegex.matcher(text);
        Match match = new Match();
        if (m.find()) {
            match.setSubject(text);
            match.setGrok(this);
            match.setMatch(m);
            match.setStart(m.start(0));
            match.setEnd(m.end(0));
        }
        return match;
    }

    // compile log format to regex
    /**
     * Compile the {@code Grok} pattern to named regex pattern.
     *
     * @param pattern : Grok pattern (ex: %{IP})
     * @throws GrokException runtime expt
     */
    public void compile(String pattern)
            throws GrokException
    {
        compile(pattern, false);
    }

    /**
     * Compile the {@code Grok} pattern to named regex pattern.
     *
     * @param pattern : Grok pattern (ex: %{IP})
     * @param namedOnly : Whether to capture named expressions only or not (i.e. %{IP:ip} but not ${IP})
     * @throws GrokException runtime expt
     */
    public void compile(String pattern, boolean namedOnly)
            throws GrokException
    {
        if (pattern == null || pattern.isBlank()) {
            throw new GrokException("{pattern} should not be empty");
        }
        namedRegexCollection.clear(); // when the grok object compiles the second format, named patterns in last format should not influence the current one.
        namedRegex = renameNamedCaptureGroups(pattern);
        originalGrokPattern = pattern;
        int index = 0;
        // flag for infinite recursion
        int iterationLeft = 1000;
        boolean continueIteration = true;

        // Replace %{foo} with the regex (mostly groupname regex)
        // and then compile the regex
        while (continueIteration) {
            continueIteration = false;
            if (iterationLeft <= 0) {
                throw new GrokException("Deep recursion pattern compilation of " + originalGrokPattern);
            }
            iterationLeft--;

            Matcher m = GrokNamedGroupExtractor.GROK_PATTERN.matcher(namedRegex);
            // Match %{Foo:bar} -> pattern name and subname
            // Match %{Foo=regex} -> add new regex definition
            if (m.find()) {
                continueIteration = true;
                Map<String, String> group = GrokNamedGroupExtractor.namedGroups(m, GROK_PATTERN_NAMED_GROUPS);
                if (group.get("definition") != null) {
                    try {
                        addPattern(group.get("pattern"), group.get("definition"));
                        group.put("name", group.get("name") + "=" + group.get("definition"));
                    }
                    catch (GrokException e) {
                        throw new GrokException("Invalid custom definition:" + e.getMessage());
                    }
                }
                // if no such pattern, throw exception
                if (!grokPatternDefinition.containsKey(group.get("pattern"))) {
                    throw new GrokException("Pattern " + group.get("pattern") + " is not defined.");
                }
                String replacement = String.format("(?<name%d>%s)", index, grokPatternDefinition.get(group.get("pattern")));
                if (namedOnly && group.get("subname") == null) {
                    replacement = grokPatternDefinition.get(group.get("pattern"));
                }
                namedRegexCollection.put("name" + index,
                        (group.get("subname") != null ? group.get("subname") : group.get("name")));
                namedRegex = namedRegex.replace("%{" + group.get("name") + "}", replacement);
                // use grokPatternPatterns map to store the actual pattern of each field in order to check its default data type when doing conversion.
                grokPatternPatterns.put(group.get("subname") != null ? group.get("subname") : group.get("name"), group.get("pattern"));
                index++;
            }
        }

        if (namedRegex.isEmpty()) {
            throw new GrokException("Pattern not found");
        }
        // Compile the regex
        compiledNamedRegex = Pattern.compile(namedRegex);
        namedGroups = GrokNamedGroupExtractor.getNameGroups(namedRegex);
    }

    private static String renameNamedCaptureGroups(String namedRegex)
    {
        // Pattern.compile() does not support underscores in named regex groups so need to remove all of them
        Matcher groupMatcher = RENAMED_CAPTURE_GROUP_PATTERN.matcher(namedRegex);
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (groupMatcher.find()) {
            groupMatcher.appendReplacement(result, "(?<" + "name" + i + ">");
            i++;
        }
        groupMatcher.appendTail(result);
        return result.toString();
    }

    /**
     * Copy the given Map of patterns (pattern name, regular expression) to {@code Grok},
     * duplicate element will be overridden.
     *
     * @param cpy : Map to copy
     * @throws GrokException runtime expt
     **/
    public void copyPatterns(Map<String, String> cpy)
            throws GrokException
    {
        if (cpy == null) {
            throw new GrokException("Invalid Patterns");
        }

        if (cpy.isEmpty()) {
            throw new GrokException("Invalid Patterns");
        }
        for (Map.Entry<String, String> entry : cpy.entrySet()) {
            grokPatternDefinition.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Get the named regex from the {@code Grok} pattern. <br>
     * See {@link #compile(String)} for more detail.
     *
     * @return named regex
     */
    public String getNamedRegex()
    {
        return namedRegex;
    }

    public boolean getStrictMode()
    {
        return strictMode;
    }

    public void setStrictMode(boolean strictMode)
    {
        this.strictMode = strictMode;
    }

    /**
     * Original grok pattern used to compile to the named regex.
     *
     * @return String Original Grok pattern
     */
    public String getOriginalGrokPattern()
    {
        return originalGrokPattern;
    }

    /**
     * Get the named regex from the given id.
     *
     * @param id : named regex id
     * @return String of the named regex
     */
    public String getNamedRegexCollectionById(String id)
    {
        return namedRegexCollection.get(id);
    }

    /**
     * Get the full collection of the default pattern data type
     *
     * @return {pattern, default datatype} map
     */
    public Map<String, String> getGrokPatternDefaultDatatype()
    {
        return grokPatternDefaultDatatype;
    }

    /**
     * Get the full collection of the actual pattern
     *
     * @return {subname, actual pattern} map
     */
    public Map<String, String> getGrokPatternPatterns()
    {
        return grokPatternPatterns;
    }

    /**
     * Get the full collection of the date format
     *
     * @return {date type, list of possible date formats} map
     */
    public Map<String, ArrayList<String>> getGrokDateFormats()
    {
        return grokDateFormats;
    }
}
