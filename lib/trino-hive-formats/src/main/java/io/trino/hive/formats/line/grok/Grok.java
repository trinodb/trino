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
import io.trino.hive.formats.line.grok.exception.GrokException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /** only use in grok discovery. */
    private String savedPattern;

    /**
     * used to identify which config file is loading.
     */
    private static final String PATTERNCONFIG = "grokPatternDefinition";
    private static final String DATATYPECONFIG = "grokPatternDefaultDatatype";
    private static final String DATEFORMATCONFIG = "grokDateFormats";

    private static final Map<String, String> DEFAULT_PATTERNS = new TreeMap<>();
    private static final Map<String, String> DEFAULT_DATA_TYPES = new HashMap<>();
    private static final Map<String, ArrayList<String>> DEFAULT_DATE_FORMATS = new HashMap<>();

    static {
        try {
            // Load configurations statically without creating an instance
            addConfFromReader(getFileFromResouces("grok/patterns"), PATTERNCONFIG);
            addConfFromReader(getFileFromResouces("grok/datatype"), DATATYPECONFIG);
            addConfFromReader(getFileFromResouces("grok/dateformat"), DATEFORMATCONFIG);
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Create Empty {@code Grok}.
     */
    public static final Grok EMPTY = new Grok();

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
        grokPatternDefinition = new TreeMap<>();
        grokPatternDefaultDatatype = new HashMap<>();
        grokPatternPatterns = new HashMap<>();
        grokDateFormats = new HashMap<>();
        namedRegexCollection = new TreeMap<>();
        savedPattern = "";
        strictMode = false;
    }

    /**
     * Create a {@code Grok} instance using the default pattern, datatype and dateformat file.
     *
     * @return {@code Grok} instance
     * @throws GrokException runtime expt
     */
    public static Grok create()
            throws GrokException
    {
        return create(null);
    }

    /**
     * Create a {@code Grok} instance with a {@code Grok} pattern, using the default pattern, datatype and dateformat file.
     *
     * @param grokExpression - <b>OPTIONAL</b> - Grok pattern to compile ex: %{APACHELOG}
     * @return {@code Grok} instance
     * @throws GrokException runtime expt
     */
    public static Grok create(String grokExpression)
            throws GrokException
    {
        Grok g = new Grok();
        g.grokPatternDefinition = DEFAULT_PATTERNS;
        g.grokPatternDefaultDatatype = DEFAULT_DATA_TYPES;
        g.grokDateFormats = DEFAULT_DATE_FORMATS;
        // compile the log format if not null
        if (grokExpression != null && !grokExpression.isEmpty()) {
            g.compile(grokExpression, false);
        }
        return g;
    }

    /**
     * Read file from resources as stream reader
     *
     * @param filePath the file path in resources, e.g. patterns/patterns
     * @return the reader that contains specific file content
     * @throws GrokException runtime expt
     */
    static Reader getFileFromResouces(String filePath)
            throws GrokException
    {
        Reader reader = new InputStreamReader(Grok.class.getClassLoader().getResourceAsStream(filePath), Charset.defaultCharset());
        if (reader == null) {
            throw new GrokException("File <" + filePath + "> not found.");
        }
        return reader;
    }

    // config file loader
    /**
     *
     * @param name Pattern Name
     * @param conf Config value to be added
     * @param destination which config file is currently operated
     * @throws GrokException grok runtime exception
     */
    private static void addConf(String name, String conf, String destination)
            throws GrokException
    {
        if (name == null || name.isBlank()) {
            throw new GrokException("Invalid pattern name when loading config file");
        }
        if (conf == null || conf.isBlank()) {
            throw new GrokException("Invalid value when loading config file");
        }
        if (destination.equals(PATTERNCONFIG)) {
            DEFAULT_PATTERNS.put(name, conf);
        }
        else if (destination.equals(DATATYPECONFIG)) {
            DEFAULT_DATA_TYPES.put(name, conf);
        }
        else {
            if (DEFAULT_DATE_FORMATS.containsKey(name)) {
                DEFAULT_DATE_FORMATS.get(name).add(conf);
            }
            else {
                ArrayList<String> formatContainer = new ArrayList<>();
                formatContainer.add(conf);
                DEFAULT_DATE_FORMATS.put(name, formatContainer);
            }
        }
    }

    /**
     * Add custom pattern to grok in the runtime.
     *
     * @param name : Pattern Name
     * @param pattern : Regular expression Or {@code Grok} pattern
     * @throws GrokException runtime expt
     **/
    public void addPattern(String name, String pattern)
            throws GrokException
    {
        addConf(name, pattern, PATTERNCONFIG);
    }

    /**
     * read config info from the given file
     *
     * @param file config file path
     * @param destination which config file is currently operated
     * @throws GrokException grok runtime exception
     */
    private void addConfFromFile(String file, String destination)
            throws GrokException
    {
        if (file == null || file.isBlank()) {
            throw new GrokException("Config file name should not be empty or null");
        }
        File f = new File(file);
        if (!f.exists()) {
            throw new GrokException("Pattern file not found");
        }
        if (!f.canRead()) {
            throw new GrokException("Pattern file cannot be read");
        }
        BufferedReader r = null;
        try {
            r = Files.newBufferedReader(f.toPath());
            addConfFromReader(r, destination);
        }
        catch (@SuppressWarnings("hiding") IOException e) {
            throw new GrokException(e.getMessage());
        }
        finally {
            try {
                if (r != null) {
                    r.close();
                }
            }
            catch (IOException io) {
                throw new GrokException("Fail to close the file.");
            }
        }
    }

    /**
     * Add patterns to {@code Grok} from the given file.
     *
     * @param file : Path of the grok pattern
     * @throws GrokException runtime expt
     */
    public void addPatternFromFile(String file)
            throws GrokException
    {
        addConfFromFile(file, PATTERNCONFIG);
    }

    /**
     * read config info from the given reader
     *
     * @param reader reader that contains config info
     * @param destination config name
     * @throws GrokException grok runtime exception
     */
    private static void addConfFromReader(Reader reader, String destination)
            throws GrokException
    {
        try (BufferedReader br = new BufferedReader(reader)) {
            String line;
            Pattern pattern = Pattern.compile("^([A-z0-9_]+)\\s+(.*)$");
            String key = null;
            while ((line = br.readLine()) != null) {
                if (destination.equals(PATTERNCONFIG) || destination.equals(DATATYPECONFIG)) {
                    // We dont want \n and commented line
                    Matcher m = pattern.matcher(line);
                    if (m.matches()) {
                        addConf(m.group(1), m.group(2), destination);
                    }
                }
                else {
                    if (line.isEmpty()) {
                        key = null;
                    }
                    else if (key == null) {
                        key = line;
                    }
                    else {
                        addConf(key, line, destination);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new GrokException(e.getMessage());
        }
    }

    /**
     * Add patterns to {@code Grok} from a Reader.
     *
     * @param r : Reader with {@code Grok} patterns
     * @throws GrokException runtime expt
     */
    public void addPatternFromReader(Reader r)
            throws GrokException
    {
        addConfFromReader(r, PATTERNCONFIG);
    }

    // match log with regex and capture results
    /**
     * Match the given <var>log</var> with the named regex.
     * And return the json representation of the matched element
     *
     * @param log : log to match
     * @return json representation og the log
     */
    public String capture(String log)
            throws GrokException, JsonProcessingException
    {
        Match match = match(log);
        match.captures();
        return match.toJson();
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
        if (compiledNamedRegex == null || text == null || text.isBlank()) {
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

            Matcher m = GrokUtils.GROK_PATTERN.matcher(namedRegex);
            // Match %{Foo:bar} -> pattern name and subname
            // Match %{Foo=regex} -> add new regex definition
            if (m.find()) {
                continueIteration = true;
                Map<String, String> group = GrokUtils.namedGroups(m, m.group());
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
    }

    private static String renameNamedCaptureGroups(String namedRegex)
    {
        // Pattern.compile() does not support underscores in named regex groups so need to remove all of them
        Pattern groupPattern = Pattern.compile("\\(\\?<([^>]+)>");
        Matcher groupMatcher = groupPattern.matcher(namedRegex);
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
     * Get the current map of {@code Grok} pattern.
     *
     * @return Patterns (name, regular expression)
     */
    public Map<String, String> getPatterns()
    {
        return grokPatternDefinition;
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

    public String getSaved_pattern()
    {
        return savedPattern;
    }

    public void setSaved_pattern(String savedPattern)
    {
        this.savedPattern = savedPattern;
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
