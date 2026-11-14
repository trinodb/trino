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
package io.trino.type;

import io.airlift.joni.Matcher;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.operator.scalar.JoniRegexpReplaceLambdaFunction;
import io.trino.operator.scalar.Regexp;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import static java.util.Objects.requireNonNull;

public final class JoniRegexp
        implements Regexp
{
    private final Slice pattern;
    private final Regex regex;

    public JoniRegexp(Slice pattern, Regex regex)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.regex = requireNonNull(regex, "regex is null");
    }

    public Slice pattern()
    {
        return pattern;
    }

    public Regex regex()
    {
        return regex;
    }

    public Matcher matcher(byte[] bytes)
    {
        return regex.matcher(bytes);
    }

    @Override
    public boolean like(Slice source)
    {
        return JoniRegexpFunctions.regexpLike(source, this);
    }

    @Override
    public Slice replace(Slice source, Slice replacement)
    {
        return JoniRegexpFunctions.regexpReplace(source, this, replacement);
    }

    @Override
    public Slice replace(Slice source, UnaryFunctionInterface replacer, BufferedArrayValueBuilder arrayValueBuilder)
    {
        return JoniRegexpReplaceLambdaFunction.regexpReplace(source, this, replacer, arrayValueBuilder);
    }

    @Override
    public Block extractAll(Slice source, long group)
    {
        return JoniRegexpFunctions.regexpExtractAll(source, this, group);
    }

    @Override
    public Slice extract(Slice source, long group)
    {
        return JoniRegexpFunctions.regexpExtract(source, this, group);
    }

    @Override
    public Block split(Slice source)
    {
        return JoniRegexpFunctions.regexpSplit(source, this);
    }

    @Override
    public long position(Slice source, long start, long occurrence)
    {
        return JoniRegexpFunctions.regexpPosition(source, this, start, occurrence);
    }

    @Override
    public long count(Slice source)
    {
        return JoniRegexpFunctions.regexpCount(source, this);
    }

    @Override
    public String toString()
    {
        return pattern.toStringUtf8();
    }
}
