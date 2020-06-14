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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.connector.CatalogName;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.UDFXPathUtil;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.VarcharType;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.type.UnknownType.UNKNOWN;

public final class XPathFunctions
{
    private static final UDFXPathUtil xpath = new UDFXPathUtil();
    private static final List<String> result = new ArrayList<String>(10);
    private static final List<String> emptyResult = Collections.<String>emptyList();

    private XPathFunctions() {}

    @SqlNullable
    @Description("Returns the text contents of the first xml node that matches the xpath expression")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice xpathString(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        String s = xpath.evalString(xml.toStringUtf8(), path.toStringUtf8());
        if (s == null) {
            return null;
        }
        return utf8Slice(s);
    }

    @SqlNullable
    @Description("Returns a string array of values within xml nodes that match the xpath expression")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("array(unknown)")
    public static Block xpath(@SqlType("varchar(x)") Slice xml, @SqlType("varchar(y)") Slice path)
    {
        List<String> initList = eval(xml.toStringUtf8(), path.toStringUtf8());

        //VarcharType varchar = createUnboundedVarcharType();
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, initList.size());
        for (String value : initList) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeSlice(builder, utf8Slice(value));
            }
        }
        return builder.build();

        /*
        //String all = String.join(",", initList.replaceAll(",","><"););
        String delim = ",";
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < initList.size() - 1) {
            sb.append(initList.get(i));
            sb.append(delim);
            i++;
        }
        sb.append(initList.get(i));
        String all = sb.toString();
        */
        /*
        int limit = initList.size() + 1;
        int numChars = 0;
        for (String element : initList) {
            numChars = numChars + element.length();
        }

        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, numChars);
        // If limit is one, the last and only element is the complete string
        if (limit == 1) {
            VARCHAR.writeSlice(parts, utf8Slice(initList.get(0)));
            return parts.build();
        }

        for (String element : initList) {
            VARCHAR.writeSlice(parts, utf8Slice(element));
        }

        return parts.build();
        */
    }

    private static List<String> eval(String xml, String path) {
        NodeList nodeList = xpath.evalNodeList(xml, path);
        if (nodeList == null) {
            return emptyResult;
        }

        result.clear();
        for (int i = 0; i < nodeList.getLength(); i++) {
            String value = nodeList.item(i).getNodeValue();
            if (value != null) {
                result.add(value);
            }
        }

        return result;
    }
}
