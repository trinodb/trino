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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

/**
 * A compiled regular expression, independent of the regex library that implements it.
 */
public interface Regexp
{
    boolean like(Slice source);

    Slice replace(Slice source, Slice replacement);

    Slice replace(Slice source, UnaryFunctionInterface replacer, BufferedArrayValueBuilder arrayValueBuilder);

    Block extractAll(Slice source, long group);

    Slice extract(Slice source, long group);

    Block split(Slice source);

    long position(Slice source, long start, long occurrence);

    long count(Slice source);
}
