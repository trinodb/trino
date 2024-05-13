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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.base.subfield.Subfield.AllSubscripts;
import io.trino.plugin.base.subfield.Subfield.NestedField;
import io.trino.plugin.base.subfield.Subfield.PathElement;
import io.trino.plugin.base.subfield.Subfield.StringSubscript;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

// Class to test pruneColumnTypeForPath. Direct referenced from Presto
public class TestPruneTypeForPath
{
    @Test
    public void fallback()
    {
        GroupType originalType = groupType("col", primitiveType("subfield1"), primitiveType("subfield2"));

        // Request non-existent field
        List<PathElement> path = singletonList(new NestedField("subField3"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(originalType.equals(actualPrunedType));
    }

    @Test
    public void noPruning()
    {
        GroupType originalType = groupType("col", primitiveType("subField1"));

        List<PathElement> path = path(nestedField("subField1"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(originalType.equals(actualPrunedType));
    }

    @Test
    public void oneLevelNesting()
    {
        GroupType originalType = groupType("col", primitiveType("subField1"), primitiveType("subField2"));

        List<PathElement> path = path(nestedField("subField2"));

        GroupType expectedPrunedType = groupType("col", primitiveType("subField2"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void multiLevelNesting()
    {
        GroupType originalType = groupType("col",
                primitiveType("subField1"),
                groupType("subField2",
                        primitiveType("subField3"),
                        primitiveType("subField4")));

        List<PathElement> path = path(nestedField("subField2"), nestedField("subField3"));

        GroupType expectedPrunedType = groupType("col",
                groupType("subField2",
                        primitiveType("subField3")));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfStructs()
    {
        GroupType originalType = groupType("col", OriginalType.LIST,
                array(
                        primitiveType("subField1"),
                        groupType("subField2",
                                primitiveType("subField3"),
                                primitiveType("subField4"))));

        List<PathElement> path = path(allSubscripts(), nestedField("subField2"),
                nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col", OriginalType.LIST,
                array(
                        groupType("subField2",
                                primitiveType("subField4"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfStructsWithSystemFieldName()
    {
        GroupType originalType = groupType("col", OriginalType.LIST,
                array(
                        primitiveType("subField1"),
                        groupType("array_element", // System field name
                                primitiveType("subField2"),
                                primitiveType("subField3"))));

        List<PathElement> path = path(allSubscripts(), nestedField("array_element"), nestedField("subField3"));

        GroupType expectedPrunedType = groupType("col", OriginalType.LIST,
                array(
                        groupType("array_element",
                                primitiveType("subField3"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfStructsType2()
    {
        GroupType originalType = groupType("col", OriginalType.LIST,
                arrayType2(
                        primitiveType("subField1"),
                        groupType("subField2",
                                primitiveType("subField3"),
                                primitiveType("subField4"))));

        List<PathElement> path = path(allSubscripts(), nestedField("subField2"), nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col", OriginalType.LIST,
                arrayType2(
                        groupType("subField2",
                                primitiveType("subField4"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfStructsType3()
    {
        GroupType originalType = groupType("col", OriginalType.LIST,
                arrayType3("col",
                        primitiveType("subField1"),
                        groupType("subField2",
                                primitiveType("subField3"),
                                primitiveType("subField4"))));

        List<PathElement> path = path(allSubscripts(), nestedField("subField2"), nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col", OriginalType.LIST,
                arrayType3("col",
                        groupType("subField2",
                                primitiveType("subField4"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfStructsType4()
    {
        GroupType originalType = groupType("col", OriginalType.LIST,
                arrayType4(
                        primitiveType("subField1"),
                        groupType("subField2",
                                primitiveType("subField3"),
                                primitiveType("subField4"))));

        List<PathElement> path = path(allSubscripts(), nestedField("subField2"), nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col", OriginalType.LIST,
                arrayType4(
                        groupType("subField2",
                                primitiveType("subField4"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void structContainingPrimitiveArray()
    {
        GroupType originalType = groupType("col",
                groupType("subField1", OriginalType.LIST, primitiveArray()),
                primitiveType("subField2"));

        List<PathElement> path = path(nestedField("subField1"), allSubscripts());

        GroupType expectedPrunedType = groupType("col",
                groupType("subField1", OriginalType.LIST, primitiveArray()));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void structContainingPrimitiveMap()
    {
        GroupType originalType = groupType("col",
                groupType("subField1", OriginalType.LIST, map(primitiveType("value"))),
                primitiveType("subField2"));

        List<PathElement> path = path(nestedField("subField1"), allSubscripts());

        GroupType expectedPrunedType = groupType("col",
                groupType("subField1", OriginalType.LIST, map(primitiveType("value"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void structContainingPrimitiveArrayType2()
    {
        GroupType originalType = groupType("col",
                groupType("subField1", OriginalType.LIST, primitiveArrayType2()),
                primitiveType("subField2"));

        List<PathElement> path = path(nestedField("subField1"), allSubscripts());

        GroupType expectedPrunedType = groupType("col",
                groupType("subField1", OriginalType.LIST, primitiveArrayType2()));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void structWithArrayOfStructs()
    {
        GroupType originalType = groupType("col",
                primitiveType("subField5"),
                groupType("arrayOfStructs", OriginalType.LIST,
                        array(
                                primitiveType("subField1"),
                                groupType("subField2",
                                        primitiveType("subField3"),
                                        primitiveType("subField4")))));

        List<PathElement> path = path(nestedField("arrayOfStructs"), allSubscripts(), nestedField("subField2"),
                nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col",
                groupType("arrayOfStructs", OriginalType.LIST,
                        array(
                                groupType("subField2",
                                        primitiveType("subField4")))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void pathTerminatedAtArray()
    {
        GroupType originalType = groupType("col",
                primitiveType("subField5"),
                groupType("arrayOfStructs", OriginalType.LIST,
                        array(
                                primitiveType("subField1"),
                                groupType("subField2",
                                        primitiveType("subField3"),
                                        primitiveType("subField4")))));

        List<PathElement> path = path(nestedField("arrayOfStructs"), allSubscripts());

        GroupType expectedPrunedType = groupType("col",
                groupType("arrayOfStructs", OriginalType.LIST,
                        array(
                                primitiveType("subField1"),
                                groupType("subField2",
                                        primitiveType("subField3"),
                                        primitiveType("subField4")))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfArrayOfStructs()
    {
        GroupType originalType = groupType("col",
                primitiveType("subField1"),
                groupType("arrayOfArrays", OriginalType.LIST,
                        array(OriginalType.LIST, array(
                                groupType("subField2",
                                        primitiveType("subField3"),
                                        primitiveType("subField4")),
                                primitiveType("subField5")))));

        List<PathElement> path = path(nestedField("arrayOfArrays"), allSubscripts(), allSubscripts(),
                nestedField("subField2"), nestedField("subField4"));

        GroupType expectedPrunedType = groupType("col",
                groupType("arrayOfArrays", OriginalType.LIST,
                        array(OriginalType.LIST, array(
                                groupType("subField2",
                                        primitiveType("subField4"))))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void arrayOfArrayOfStructsWithSystemFieldName()
    {
        GroupType originalType = groupType("col",
                primitiveType("subField1"),
                groupType("arrayOfArrays", OriginalType.LIST,
                        array(OriginalType.LIST, array(
                                groupType("array_element",
                                        primitiveType("subField2"),
                                        primitiveType("array_element")),
                                primitiveType("subField4")))));

        List<PathElement> path = path(nestedField("arrayOfArrays"), allSubscripts(), allSubscripts(),
                nestedField("array_element"), nestedField("array_element"));

        GroupType expectedPrunedType = groupType("col",
                groupType("arrayOfArrays", OriginalType.LIST,
                        array(OriginalType.LIST, array(
                                groupType("array_element",
                                        primitiveType("array_element"))))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void mapOfStruct()
    {
        GroupType originalType = groupType("col", OriginalType.MAP,
                map(groupType("value",
                        primitiveType("subField1"),
                        primitiveType("subField2"))));

        List<PathElement> path = path(subscript("index"), nestedField("subField2"));

        GroupType expectedPrunedType = groupType("col", OriginalType.MAP,
                map(groupType("value",
                        primitiveType("subField2"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void mapOfStructType2()
    {
        GroupType originalType = groupType("col", OriginalType.MAP_KEY_VALUE,
                mapType2(groupType("value",
                        primitiveType("subField1"),
                        primitiveType("subField2"))));

        List<PathElement> path = path(subscript("index"), nestedField("subField2"));

        GroupType expectedPrunedType = groupType("col", OriginalType.MAP_KEY_VALUE,
                mapType2(groupType("value",
                        primitiveType("subField2"))));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, path);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void caseInsensitivityForLowercasePath()
    {
        GroupType originalType = groupType("col", OriginalType.MAP_KEY_VALUE,
                primitiveType("subField1"),
                primitiveType("subField2"));

        List<PathElement> lowercasePath = path(nestedField("subfield2"));

        GroupType expectedPrunedType = groupType("col", OriginalType.MAP_KEY_VALUE, primitiveType("subField2"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, lowercasePath);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void caseInsensitivityForUppercasePath()
    {
        GroupType originalType = groupType("col", OriginalType.MAP_KEY_VALUE,
                primitiveType("subField1"),
                primitiveType("subField2"));

        List<PathElement> uppercasePath = path(nestedField("SUBFIELD2"));

        GroupType expectedPrunedType = groupType("col", OriginalType.MAP_KEY_VALUE, primitiveType("subField2"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, uppercasePath);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    @Test
    public void caseInsensitivityForMixedcasePath()
    {
        GroupType originalType = groupType("col", OriginalType.MAP_KEY_VALUE,
                primitiveType("subField1"),
                primitiveType("subField2"));

        List<PathElement> mixedcasePath = path(nestedField("sUbfiEld2"));

        GroupType expectedPrunedType = groupType("col", OriginalType.MAP_KEY_VALUE, primitiveType("subField2"));

        Type actualPrunedType = ParquetPageSourceFactory.pruneColumnTypeForPath(originalType, mixedcasePath);
        assertThat(expectedPrunedType.equals(actualPrunedType));
    }

    private static GroupType groupType(String name, Type... fields)
    {
        return new GroupType(Type.Repetition.OPTIONAL, name, fields);
    }

    private static GroupType groupType(String name, OriginalType originalType, Type... fields)
    {
        return new GroupType(Type.Repetition.OPTIONAL, name, originalType, fields);
    }

    private static GroupType array(Type... fields)
    {
        return new GroupType(Type.Repetition.REPEATED, "bag",
                new GroupType(Type.Repetition.OPTIONAL, "array_element", fields));
    }

    private static GroupType array(OriginalType originalType, Type... fields)
    {
        return new GroupType(Type.Repetition.REPEATED, "bag",
                new GroupType(Type.Repetition.OPTIONAL, "array_element", originalType, fields));
    }

    // Two-level group to represent array instead of 3 with known name 'array' for second level
    private static GroupType arrayType2(Type... fields)
    {
        return new GroupType(Type.Repetition.REPEATED, "array", fields);
    }

    // Two-level group to represent array instead of 3 with known name '{parent}_tuple' for second level
    private static GroupType arrayType3(String parentFieldName, Type... fields)
    {
        return new GroupType(Type.Repetition.REPEATED, parentFieldName + "_tuple", fields);
    }

    // Two-level group to represent array instead of 3 identified since normally the second level has exactly one child
    // but this case has multiple
    private static GroupType arrayType4(Type... fields)
    {
        return new GroupType(Type.Repetition.REPEATED, "element", fields);
    }

    private static GroupType primitiveArray()
    {
        return new GroupType(Type.Repetition.REPEATED, "bag",
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "array_element"));
    }

    private static PrimitiveType primitiveArrayType2()
    {
        return new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT32, "element");
    }

    private static GroupType map(Type valueType)
    {
        return new GroupType(Type.Repetition.REPEATED, "map", OriginalType.MAP_KEY_VALUE, primitiveType("key"), valueType);
    }

    private static GroupType mapType2(Type valueType)
    {
        return new GroupType(Type.Repetition.REPEATED, "map", primitiveType("key"), valueType);
    }

    private static Type primitiveType(String name)
    {
        return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, name);
    }

    private static List<PathElement> path(PathElement... elements)
    {
        return asList(elements);
    }

    private static NestedField nestedField(String name)
    {
        return new NestedField(name);
    }

    private static AllSubscripts allSubscripts()
    {
        return AllSubscripts.getInstance();
    }

    private static StringSubscript subscript(String index)
    {
        return new StringSubscript(index);
    }
}
