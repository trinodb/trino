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
package io.trino.sql.tree;

import io.trino.sql.parser.ParsingException;

import static java.util.Objects.requireNonNull;

public class NumericParser
{
    private NumericParser() {}

    public static long parseNumeric(String value, NodeLocation location)
    {
        requireNonNull(value, "value is null");
        try {
            return parseNumeric(value);
        }
        catch (NumberFormatException e) {
            throw new ParsingException("Invalid numeric literal: " + value, location);
        }
    }

    private static long parseNumeric(String value)
    {
        value = value.replace("_", "");

        if (value.startsWith("0x") || value.startsWith("0X")) {
            return Long.parseLong(value.substring(2), 16);
        }
        else if (value.startsWith("-0x") || value.startsWith("-0X")) {
            return Long.parseLong("-" + value.substring(3), 16);
        }
        else if (value.startsWith("0b") || value.startsWith("0B")) {
            return Long.parseLong(value.substring(2), 2);
        }
        else if (value.startsWith("-0b") || value.startsWith("-0B")) {
            return Long.parseLong("-" + value.substring(3), 2);
        }
        else if (value.startsWith("0o") || value.startsWith("0O")) {
            return Long.parseLong(value.substring(2), 8);
        }
        else if (value.startsWith("-0o") || value.startsWith("-0O")) {
            return Long.parseLong("-" + value.substring(3), 8);
        }
        else {
            return Long.parseLong(value);
        }
    }
}
