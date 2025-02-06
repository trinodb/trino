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
import io.trino.hive.formats.line.grok.exception.GrokError;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Set of Groks instance
 *
 * @author anthonycorbacho
 * @since 0.0.1
 */
// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
@Deprecated
public class Pile
{
    // Private
    private List<Grok> groks;
    private Map<String, String> patterns;
    private List<String> patternFiles;

    static final String defaultPatternDirectory = "patterns/";

    /**
     ** Constructor
     **/
    public Pile()
    {
        patterns = new TreeMap<String, String>();
        groks = new ArrayList<Grok>();
        patternFiles = new ArrayList<String>();
    }

    /**
     *
     * @param name of the pattern
     * @param file path
     * @return success or not
     */
    public int addPattern(String name, String file)
    {
        if (name.isEmpty() || file.isEmpty()) {
            return GrokError.GROK_ERROR_UNINITIALIZED;
        }
        patterns.put(name, file);
        return GrokError.GROK_OK;
    }

    /**
     * Load patterns file from a directory
     *
     * @param directory directory to load pattern files from
     * @return success or not
     */
    public int addFromDirectory(String directory)
    {
        if (directory == null || directory.isEmpty()) {
            directory = defaultPatternDirectory;
        }

        File dir = new File(directory);
        File[] lst = dir.listFiles();

        for (int i = 0; i < lst.length; i++) {
            if (lst[i].isFile()) {
                addPatternFromFile(lst[i].getAbsolutePath());
            }
        }

        return GrokError.GROK_OK;
    }

    /**
     * Add pattern to grok from a file
     *
     * @param file file to add to grok
     * @return success or not
     */
    public int addPatternFromFile(String file)
    {
        File f = new File(file);
        if (!f.exists()) {
            return GrokError.GROK_ERROR_FILE_NOT_ACCESSIBLE;
        }
        patternFiles.add(file);
        return GrokError.GROK_OK;
    }

    /**
     * Compile the pattern with a corresponding grok
     *
     * @param pattern inputPattern for Grok
     * @throws Throwable GrokException
     */
    public void compile(String pattern)
            throws Throwable
    {
        Grok grok = new Grok();

        Map<String, String> map = new TreeMap<String, String>();

        for (Map.Entry<String, String> entry : patterns.entrySet()) {
            if (!map.containsValue(entry.getValue())) {
                grok.addPattern(entry.getKey(), entry.getValue());
            }
        }

        for (String file : patternFiles) {
            grok.addPatternFromFile(file);
        }

        grok.compile(pattern);
        groks.add(grok);
    }

    /**
     * @param line to match
     * @return Grok Match
     */
    public Match match(String line)
    {
        for (Grok grok : groks) {
            Match gm = grok.match(line);
            if (!gm.isNull()) {
                return gm;
            }
        }

        return null;
    }
}
