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
package io.trino.operator.scalar.xpath;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import jakarta.annotation.Nullable;
import org.w3c.dom.Node;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class XPathFunctions
{
    private XPathFunctions() {}

    @SqlNullable
    @Description("Returns a string array of values within xml nodes that match the xpath expression")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar)")
    public static Block xpath(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        XPathHelper xpathHelper = new XPathHelper();
        List<String> xpathNodeValueList = xpathHelper.evalNodeList(xml.toStringUtf8(), path.toStringUtf8())
                .map(nodeList -> Stream.iterate(0, i -> i + 1)
                        .limit(nodeList.getLength())
                        .map(nodeList::item)
                        .filter(node -> node.getNodeValue() != null)
                        .map(Node::getNodeValue)
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, xpathNodeValueList.size());
        xpathNodeValueList.stream().forEach(value -> VARCHAR.writeSlice(blockBuilder, utf8Slice(value)));
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("Returns the first node that matches the xpath expression")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice xpathNode(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        XPathHelper xpathHelper = new XPathHelper();
        Node xpathNode = xpathHelper.evalNode(xml.toStringUtf8(), path.toStringUtf8());
        return xpathNode == null ? null : slice(xpathNode.getNodeValue());
    }

    @SqlNullable
    @Description("Returns a string from the xml body computed based on the provided path")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice xpathString(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        XPathHelper xpathHelper = new XPathHelper();
        return slice(xpathHelper.evalString(xml.toStringUtf8(), path.toStringUtf8()));
    }

    @SqlNullable
    @Description("Returns true if the XPath expression evaluates to true, or if a matching node is found")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean xpathBoolean(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        XPathHelper xPathHelper = new XPathHelper();
        return xPathHelper.evalBoolean(xml.toStringUtf8(), path.toStringUtf8());
    }

    @SqlNullable
    @Description("Return a double value, or the value 0.0 if no match is found, or NaN for non-numeric match")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.DOUBLE)
    public static Double xpathDouble(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        XPathHelper xPathHelper = new XPathHelper();
        return xPathHelper.evalNumber(xml.toStringUtf8(), path.toStringUtf8());
    }

    private static Slice slice(@Nullable String data)
    {
        return data == null ? null : utf8Slice(data);
    }
}
