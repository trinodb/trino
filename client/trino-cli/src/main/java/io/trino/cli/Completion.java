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
package io.trino.cli;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

import java.util.List;

import static java.util.Arrays.asList;

public final class Completion
{
    private Completion() {}

    public static Completer commandCompleter()
    {
        return new AggregateCompleter(buildArgumentCompleter("ALTER", asList("SCHEMA", "TABLE")),
                buildArgumentCompleter("CREATE", asList("SCHEMA", "TABLE")),
                buildArgumentCompleter("DESCRIBE"),
                buildArgumentCompleter("DROP", asList("SCHEMA", "TABLE")),
                buildArgumentCompleter("EXPLAIN"),
                buildArgumentCompleter("HELP"),
                buildArgumentCompleter("QUIT"),
                buildArgumentCompleter("SELECT"),
                buildArgumentCompleter("SHOW", asList("CATALOGS", "COLUMNS", "FUNCTIONS", "SCHEMAS", "SESSION", "TABLES")),
                buildArgumentCompleter("USE"));
    }

    private static Completer buildArgumentCompleter(String command)
    {
        // NullCompleter is used to indicate the command is complete and the last word should not be repeated
        return new ArgumentCompleter(new StringsCompleter(command), NullCompleter.INSTANCE);
    }

    private static Completer buildArgumentCompleter(String command, List<String> options)
    {
        return new ArgumentCompleter(new StringsCompleter(command), new StringsCompleter(options), NullCompleter.INSTANCE);
    }
}
