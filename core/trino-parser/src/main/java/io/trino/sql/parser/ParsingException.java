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
package io.trino.sql.parser;

import io.trino.sql.tree.NodeLocation;
import org.antlr.v4.runtime.RecognitionException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ParsingException
        extends RuntimeException
{
    private final int line;
    private final int column;

    public ParsingException(String message, RecognitionException cause, int line, int column)
    {
        super(message, cause);
        checkArgument(line > 0, "line must be > 0");
        checkArgument(column > 0, "column must be > 0");

        this.line = line;
        this.column = column;
    }

    public ParsingException(String message)
    {
        this(message, null, 1, 1);
    }

    public ParsingException(String message, RecognitionException cause)
    {
        this(message, cause, 1, 1);
    }

    public ParsingException(String message, NodeLocation nodeLocation)
    {
        this(message, null, nodeLocation.getLineNumber(), nodeLocation.getColumnNumber());
    }

    public int getLineNumber()
    {
        return line;
    }

    public int getColumnNumber()
    {
        return column;
    }

    public String getErrorMessage()
    {
        return super.getMessage();
    }

    @Override
    public String getMessage()
    {
        return format("line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}
