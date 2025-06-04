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
package io.trino.sql;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.grammar.sql.SqlKeywords.sqlKeywords;

public final class ReservedIdentifiers
{
    private static final Pattern TABLE_ROW = Pattern.compile("\\| `([A-Z_]+)`.*");
    private static final String TABLE_START = "| ------------------- |";
    private static final String TABLE_ROW_START = "|";

    private static final SqlParser PARSER = new SqlParser();

    private ReservedIdentifiers() {}

    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(String[] args)
    {
        if ((args.length == 2) && args[0].equals("validateDocs")) {
            try {
                validateDocs(Paths.get(args[1]));
            }
            catch (Throwable t) {
                t.printStackTrace();
                System.exit(100);
            }
        }
        else {
            for (String name : reservedIdentifiers()) {
                System.out.println(name);
            }
        }
    }

    private static void validateDocs(Path path)
            throws IOException
    {
        System.out.println("Validating " + path);
        List<String> lines = Files.readAllLines(path);

        if (lines.stream().filter(s -> s.startsWith(TABLE_START)).count() != 1) {
            throw new RuntimeException("Failed to find exactly one table");
        }

        Iterator<String> iterator = lines.iterator();

        // find start of list in table
        while (!iterator.next().startsWith(TABLE_START)) {
            // skip
        }

        Set<String> reserved = reservedIdentifiers();
        Set<String> found = new HashSet<>();
        while (true) {
            String line = iterator.next();
            if (!line.startsWith(TABLE_ROW_START)) {
                break;
            }

            Matcher matcher = TABLE_ROW.matcher(line);
            if (!matcher.matches()) {
                throw new RuntimeException("Invalid table line: " + line);
            }
            String name = matcher.group(1);

            if (!reserved.contains(name)) {
                throw new RuntimeException("Documented identifier is not reserved: " + name);
            }
            if (!found.add(name)) {
                throw new RuntimeException("Duplicate documented identifier: " + name);
            }
        }

        for (String name : reserved) {
            if (!found.contains(name)) {
                throw new RuntimeException("Reserved identifier is not documented: " + name);
            }
        }

        System.out.println("Validated %s reserved identifiers".formatted(reserved.size()));
    }

    public static Set<String> reservedIdentifiers()
    {
        return sqlKeywords().stream()
                .filter(ReservedIdentifiers::reserved)
                .sorted()
                .collect(toImmutableSet());
    }

    public static boolean reserved(String name)
    {
        try {
            return !(PARSER.createExpression(name) instanceof Identifier);
        }
        catch (ParsingException _) {
            return true;
        }
    }
}
