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
package io.trino.plugin.paimon;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.type.TypeDeserializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoColumnHandleTest
{
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));
    private final JsonCodec<PaimonColumnHandle> codec = columnHandleJsonCodec();

    private static JsonCodec<PaimonColumnHandle> columnHandleJsonCodec()
    {
        return new JsonCodecFactory(
                new JsonMapperProvider()
                        .withJsonDeserializers(Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                        .get())
                .jsonCodec(PaimonColumnHandle.class);
    }

    @Test
    public void testTrinoColumnHandle()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        testRoundTrip(expected);
    }

    @Test
    public void testNonVariantColumnHandleRejectsSerializedTrinoTypeJsonField()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = codec.toJson(expected).replace("}", ",\"trinoType\":\"varchar\"}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("trinoType must not be serialized for non-VARIANT columns");
    }

    @Test
    public void testColumnHandleRejectsUnknownJsonFields()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = codec.toJson(expected).replace("}", ",\"rowId\":false}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonColumnHandle JSON field: rowId");
    }

    @Test
    public void testColumnHandleRejectsMissingRequiredJsonFields()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = codec.toJson(expected);

        assertMissingColumnHandleJsonField(json, "columnName");
        assertMissingColumnHandleJsonField(json, "typeString");
    }

    @Test
    public void testColumnHandleRejectsBlankRequiredJsonFields()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = codec.toJson(expected);

        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(json, "columnName", "\"\"")))
                .hasRootCauseMessage("columnName is blank");
        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(json, "typeString", "\"\"")))
                .hasRootCauseMessage("typeString is blank");
    }

    @Test
    public void testColumnHandleAcceptsTrinoTypedJsonField()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"%s\"".formatted(typedHandleId(PaimonColumnHandle.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testColumnHandleRejectsInvalidTrinoTypedJsonField()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":{\"name\":\"paimon\"}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonColumnHandle JSON @type field");
    }

    @Test
    public void testColumnHandleRejectsConnectorNameOnlyTypedJsonField()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonColumnHandle JSON @type field");
    }

    @Test
    public void testExplicitNonVariantColumnHandleRequiresTrinoType()
    {
        assertThatThrownBy(() -> PaimonColumnHandle.of("name", DataTypes.STRING(), (Type) null))
                .hasMessage("trinoType is null");
        assertThatThrownBy(() -> PaimonColumnHandle.of("name", null, VARCHAR))
                .hasMessage("columnType is null");
    }

    @Test
    public void testTrinoColumnHandlePreservesBlobTypeString()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("blob_value", DataTypes.BLOB());

        PaimonColumnHandle actual = codec.fromJson(codec.toJson(expected));

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
        assertThat(actual.getTrinoType()).isEqualTo(VARBINARY);
    }

    @Test
    public void testLogicalTypeIsCached()
    {
        PaimonColumnHandle handle = PaimonColumnHandle.of("name", DataTypes.STRING());

        assertThat(handle.logicalType()).isSameAs(handle.logicalType());
    }

    @Test
    public void testNonVariantTypeManagerConstructorDoesNotSerializeTrinoType()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING(), TESTING_TYPE_MANAGER);

        String json = codec.toJson(expected);

        assertThat(json).doesNotContain("trinoType");
        assertThat(codec.fromJson(json).getTrinoType()).isEqualTo(VARCHAR);
    }

    @Test
    public void testTypeManagerConstructorRequiresTypeManager()
    {
        assertThatThrownBy(() -> PaimonColumnHandle.of(
                "name",
                DataTypes.STRING(),
                (TypeManager) null))
                .hasMessage("typeManager is null");
    }

    @Test
    public void testTrinoColumnHandlePreservesVariantTypeString()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of(
                "variant_value",
                DataTypes.VARIANT(),
                TESTING_TYPE_MANAGER);

        PaimonColumnHandle actual = codec.fromJson(codec.toJson(expected));

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
        assertThat(actual.getTrinoType()).isEqualTo(JSON_TYPE);
    }

    @Test
    public void testVariantColumnHandleWithoutTrinoTypeFails()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of(
                "variant_value",
                DataTypes.VARIANT(),
                TESTING_TYPE_MANAGER);

        String json = codec.toJson(expected).replace(",\"trinoType\":\"json\"", "");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("trinoType is required for Paimon VARIANT");
    }

    @Test
    public void testVariantColumnHandleRequiresTypeManagerConstructor()
    {
        assertThatThrownBy(() -> PaimonColumnHandle.of("variant_value", DataTypes.VARIANT()))
                .hasMessage("Paimon VARIANT requires TypeManager for Trino JSON type");
        assertThatThrownBy(() -> PaimonColumnHandle.of(
                "variant_value",
                DataTypes.VARIANT(),
                (TypeManager) null))
                .hasMessage("typeManager is null");
    }

    @Test
    public void testVariantColumnHandleRejectsNonJsonTrinoType()
    {
        assertThatThrownBy(() -> PaimonColumnHandle.of("variant_value", DataTypes.VARIANT(), VARCHAR))
                .hasMessage("trinoType must match Paimon type mapping");
    }

    @Test
    public void testNestedVariantColumnHandleRequiresNestedJsonType()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of(
                "variant_values",
                DataTypes.ARRAY(DataTypes.VARIANT()),
                TESTING_TYPE_MANAGER);

        testRoundTrip(expected);

        assertThatThrownBy(() -> PaimonColumnHandle.of(
                "variant_values",
                DataTypes.ARRAY(DataTypes.VARIANT()),
                new ArrayType(VARCHAR)))
                .hasMessage("trinoType must match Paimon type mapping");
    }

    @Test
    public void testVariantColumnHandleRejectsMismatchedNonVariantNestedType()
    {
        org.apache.paimon.types.RowType logicalType = DataTypes.ROW(
                DataTypes.FIELD(1, "payload", DataTypes.VARIANT()),
                DataTypes.FIELD(2, "count", DataTypes.INT()));
        RowType trinoType = RowType.from(List.of(
                RowType.field("payload", JSON_TYPE),
                RowType.field("count", BIGINT)));

        assertThatThrownBy(() -> PaimonColumnHandle.of("event", logicalType, trinoType))
                .hasMessage("trinoType must match Paimon type mapping");
    }

    @Test
    public void testVariantColumnHandleRejectsMismatchedRowFieldName()
    {
        org.apache.paimon.types.RowType logicalType = DataTypes.ROW(
                DataTypes.FIELD(1, "payload", DataTypes.VARIANT()),
                DataTypes.FIELD(2, "count", DataTypes.INT()));
        RowType trinoType = RowType.from(List.of(
                RowType.field("event_payload", JSON_TYPE),
                RowType.field("count", INTEGER)));

        assertThatThrownBy(() -> PaimonColumnHandle.of("event", logicalType, trinoType))
                .hasMessage("trinoType must match Paimon type mapping");
    }

    @Test
    public void testExplicitAnonymousRowTypeMatchesStablePaimonFieldNames()
    {
        org.apache.paimon.types.RowType logicalType = DataTypes.ROW(
                DataTypes.FIELD(0, "f0", DataTypes.INT()),
                DataTypes.FIELD(1, "f1", DataTypes.STRING()));
        PaimonColumnHandle handle = PaimonColumnHandle.of(
                "row_value",
                logicalType,
                RowType.anonymous(List.of(INTEGER, VARCHAR)));

        assertThat(handle.getTrinoType()).isEqualTo(PaimonTypeUtils.fromPaimonType(logicalType));
    }

    @Test
    public void testColumnHandleIdentityIncludesPaimonType()
    {
        assertThat(PaimonColumnHandle.of("value", DataTypes.INT()))
                .isNotEqualTo(PaimonColumnHandle.of("value", DataTypes.STRING()));
    }

    private void testRoundTrip(PaimonColumnHandle expected)
    {
        String json = codec.toJson(expected);
        PaimonColumnHandle actual = codec.fromJson(json);

        assertThat(json).doesNotContain("rowId");
        if (expected.getSerializedTrinoType() == null) {
            assertThat(json).doesNotContain("trinoType");
        }
        else {
            assertThat(json).contains("trinoType");
        }
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getColumnName()).isEqualTo(expected.getColumnName());
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
        assertThat(actual.getTrinoType()).isEqualTo(expected.getTrinoType());
    }

    private static String appendJsonField(String json, String field)
    {
        return json.substring(0, json.length() - 1) + "," + field + "}";
    }

    private void assertMissingColumnHandleJsonField(String json, String fieldName)
    {
        assertThatThrownBy(() -> codec.fromJson(removeJsonField(json, fieldName)))
                .rootCause()
                .hasMessageContaining("Missing required creator property '%s'".formatted(fieldName));
    }

    private static String removeJsonField(String json, String fieldName)
    {
        int fieldStart = findTopLevelJsonField(json, fieldName);
        int valueStart = json.indexOf(':', fieldStart) + 1;
        int fieldEnd = findJsonValueEnd(json, valueStart);

        int removeStart = fieldStart;
        int removeEnd = fieldEnd;
        if (fieldStart > 1 && json.charAt(fieldStart - 1) == ',') {
            removeStart = fieldStart - 1;
        }
        else if (fieldEnd < json.length() - 1 && json.charAt(fieldEnd) == ',') {
            removeEnd = fieldEnd + 1;
        }
        return json.substring(0, removeStart) + json.substring(removeEnd);
    }

    private static String replaceJsonField(String json, String fieldName, String replacementValue)
    {
        int fieldStart = findTopLevelJsonField(json, fieldName);
        int valueStart = json.indexOf(':', fieldStart) + 1;
        int fieldEnd = findJsonValueEnd(json, valueStart);
        return json.substring(0, valueStart) + replacementValue + json.substring(fieldEnd);
    }

    private static int findTopLevelJsonField(String json, String fieldName)
    {
        String quotedField = "\"" + fieldName + "\"";
        boolean inString = false;
        boolean escaped = false;
        int depth = 0;
        for (int index = 0; index < json.length(); index++) {
            char value = json.charAt(index);
            if (inString) {
                if (escaped) {
                    escaped = false;
                }
                else if (value == '\\') {
                    escaped = true;
                }
                else if (value == '"') {
                    inString = false;
                }
                continue;
            }
            if (value == '"') {
                if (depth == 1 && json.startsWith(quotedField, index)) {
                    return index;
                }
                inString = true;
            }
            else if (value == '{' || value == '[') {
                depth++;
            }
            else if (value == '}' || value == ']') {
                depth--;
            }
        }
        throw new IllegalArgumentException("JSON field not found: " + fieldName);
    }

    private static int findJsonValueEnd(String json, int valueStart)
    {
        boolean inString = false;
        boolean escaped = false;
        int depth = 0;
        for (int index = valueStart; index < json.length(); index++) {
            char value = json.charAt(index);
            if (inString) {
                if (escaped) {
                    escaped = false;
                }
                else if (value == '\\') {
                    escaped = true;
                }
                else if (value == '"') {
                    inString = false;
                }
                continue;
            }
            if (value == '"') {
                inString = true;
            }
            else if (value == '{' || value == '[') {
                depth++;
            }
            else if (value == '}' || value == ']') {
                if (depth == 0) {
                    return index;
                }
                depth--;
            }
            else if (value == ',' && depth == 0) {
                return index;
            }
        }
        return json.length() - 1;
    }

    private static String typedHandleId(Class<?> handleClass)
    {
        return "paimon:" + handleClass.getName();
    }
}
