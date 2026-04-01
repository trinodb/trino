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
package io.trino.plugin.ducklake;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses DuckDB SQLLogicTest .test files into structured blocks.
 * <p>
 * The format uses line-based directives:
 * <ul>
 *   <li>{@code statement ok} / {@code statement error} — SQL to execute</li>
 *   <li>{@code query <TYPES> [sort_mode] [label]} — SQL query with expected results after {@code ----}</li>
 *   <li>{@code require}, {@code test-env}, {@code loop}, {@code endloop}, {@code mode}, {@code skipif} — control directives</li>
 * </ul>
 */
public final class TestFileParser
{
    private static final Pattern STATEMENT_PATTERN = Pattern.compile("^statement\\s+(ok|error)$");
    private static final Pattern QUERY_PATTERN = Pattern.compile("^query\\s+([A-Z]+)(?:\\s+(.+))?$");
    private static final Pattern TEST_ENV_PATTERN = Pattern.compile("^test-env\\s+(\\S+)\\s+(.+)$");

    private TestFileParser() {}

    public sealed interface TestBlock
            permits StatementBlock, QueryBlock, TestEnvBlock, SkipBlock {}

    public record StatementBlock(String sql, boolean expectOk)
            implements TestBlock {}

    public record QueryBlock(String sql, String types, List<String> expectedRows, SortMode sortMode, String label)
            implements TestBlock {}

    public record TestEnvBlock(String name, String value)
            implements TestBlock {}

    public record SkipBlock(String directive, String content)
            implements TestBlock {}

    public enum SortMode
    {
        EXACT,
        ROWSORT,
        NOSORT
    }

    public static List<TestBlock> parse(Path testFile)
            throws IOException
    {
        List<String> lines = Files.readAllLines(testFile);
        List<TestBlock> blocks = new ArrayList<>();
        int i = 0;

        while (i < lines.size()) {
            String line = lines.get(i).stripTrailing();

            // Skip comments and blank lines
            if (line.isEmpty() || line.startsWith("#")) {
                i++;
                continue;
            }

            // statement ok / statement error
            Matcher statementMatcher = STATEMENT_PATTERN.matcher(line);
            if (statementMatcher.matches()) {
                boolean expectOk = "ok".equals(statementMatcher.group(1));
                i++;
                StringBuilder sql = new StringBuilder();
                while (i < lines.size()) {
                    String sqlLine = lines.get(i).stripTrailing();
                    if (sqlLine.isEmpty() || isDirective(sqlLine)) {
                        break;
                    }
                    // For statement error, the line after "statement error" may be expected error text after ----
                    if (sqlLine.equals("----")) {
                        i++;
                        // Skip expected error message lines
                        while (i < lines.size() && !lines.get(i).stripTrailing().isEmpty() && !isDirective(lines.get(i).stripTrailing())) {
                            i++;
                        }
                        break;
                    }
                    if (!sql.isEmpty()) {
                        sql.append("\n");
                    }
                    sql.append(sqlLine);
                    i++;
                }
                if (!sql.isEmpty()) {
                    blocks.add(new StatementBlock(sql.toString(), expectOk));
                }
                continue;
            }

            // query <TYPES> [sort_mode] [label]
            Matcher queryMatcher = QUERY_PATTERN.matcher(line);
            if (queryMatcher.matches()) {
                String types = queryMatcher.group(1);
                String rest = queryMatcher.group(2);
                SortMode sortMode = SortMode.EXACT;
                String label = null;

                if (rest != null) {
                    String[] parts = rest.trim().split("\\s+");
                    for (String part : parts) {
                        switch (part) {
                            case "rowsort" -> sortMode = SortMode.ROWSORT;
                            case "nosort" -> sortMode = SortMode.NOSORT;
                            default -> label = part;
                        }
                    }
                }

                i++;
                StringBuilder sql = new StringBuilder();
                while (i < lines.size()) {
                    String sqlLine = lines.get(i).stripTrailing();
                    if (sqlLine.equals("----") || sqlLine.isEmpty() || isDirective(sqlLine)) {
                        break;
                    }
                    if (!sql.isEmpty()) {
                        sql.append("\n");
                    }
                    sql.append(sqlLine);
                    i++;
                }

                List<String> expectedRows = new ArrayList<>();
                if (i < lines.size() && lines.get(i).stripTrailing().equals("----")) {
                    i++; // skip ----
                    while (i < lines.size()) {
                        String resultLine = lines.get(i).stripTrailing();
                        if (resultLine.isEmpty() || isDirective(resultLine)) {
                            break;
                        }
                        expectedRows.add(resultLine);
                        i++;
                    }
                }

                if (!sql.isEmpty()) {
                    blocks.add(new QueryBlock(sql.toString(), types, expectedRows, sortMode, label));
                }
                continue;
            }

            // test-env VAR VALUE
            Matcher testEnvMatcher = TEST_ENV_PATTERN.matcher(line);
            if (testEnvMatcher.matches()) {
                blocks.add(new TestEnvBlock(testEnvMatcher.group(1), testEnvMatcher.group(2)));
                i++;
                continue;
            }

            // Other directives: require, loop, endloop, mode, skipif, require-env
            if (line.startsWith("require") || line.startsWith("loop") || line.startsWith("endloop") ||
                    line.startsWith("mode") || line.startsWith("skipif") || line.startsWith("onlyif") ||
                    line.startsWith("halt")) {
                blocks.add(new SkipBlock(line.split("\\s+")[0], line));
                i++;
                continue;
            }

            // Unknown line — skip
            i++;
        }

        return blocks;
    }

    private static boolean isDirective(String line)
    {
        return line.startsWith("statement ") ||
                line.startsWith("query ") ||
                line.startsWith("require") ||
                line.startsWith("test-env ") ||
                line.startsWith("loop") ||
                line.startsWith("endloop") ||
                line.startsWith("mode ") ||
                line.startsWith("skipif ") ||
                line.startsWith("onlyif ") ||
                line.startsWith("halt");
    }
}
