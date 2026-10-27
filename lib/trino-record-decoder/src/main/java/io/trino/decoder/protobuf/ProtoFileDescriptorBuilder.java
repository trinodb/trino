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
package io.trino.decoder.protobuf;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ApiProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.SourceContextProto;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.TypeProto;
import com.google.protobuf.WrappersProto;
import com.google.type.CalendarPeriodProto;
import com.google.type.ColorProto;
import com.google.type.DateProto;
import com.google.type.DateTimeProto;
import com.google.type.DayOfWeekProto;
import com.google.type.DecimalProto;
import com.google.type.ExprProto;
import com.google.type.FractionProto;
import com.google.type.IntervalProto;
import com.google.type.LatLngProto;
import com.google.type.MoneyProto;
import com.google.type.MonthProto;
import com.google.type.PhoneNumberProto;
import com.google.type.PostalAddressProto;
import com.google.type.QuaternionProto;
import com.google.type.TimeOfDayProto;
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.Reserved;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.protobuf.TextFormat.escapeBytes;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Builds linked {@link FileDescriptor}s from {@code .proto} source text.
 * <p>
 * The text is parsed with Wire's parser and converted into {@link FileDescriptorProto}s.
 * Type references are left to protobuf-java to resolve: a named field type is emitted as
 * {@code type_name} without a {@code type}, so {@code FileDescriptor.buildFrom} decides whether
 * it is a message or an enum using the same scoping rules as {@code protoc}. Imports of the
 * well-known {@code google/protobuf/*.proto} files and of the Google common types in
 * {@code google/type/*.proto} are linked against the descriptors bundled with protobuf-java and
 * the Google common protos; every other import is fetched from the supplied {@link ImportResolver}.
 * <p>
 * Options other than those that affect validation or encoding ({@code allow_alias},
 * {@code map_entry}, {@code packed}, {@code deprecated}, {@code json_name}, {@code default})
 * are not carried into the descriptors, and {@code extend} declarations, extension ranges,
 * groups and services are skipped. None of them affect how a message decodes.
 */
public final class ProtoFileDescriptorBuilder
{
    @FunctionalInterface
    public interface ImportResolver
    {
        /**
         * Returns the source text of the file imported under the given path, or empty if there is no such file.
         */
        Optional<String> getProtoSource(String importPath);
    }

    private static final Map<String, FileDescriptor> WELL_KNOWN_FILES = Stream.of(
                    AnyProto.getDescriptor(),
                    ApiProto.getDescriptor(),
                    DescriptorProtos.getDescriptor(),
                    DurationProto.getDescriptor(),
                    EmptyProto.getDescriptor(),
                    FieldMaskProto.getDescriptor(),
                    SourceContextProto.getDescriptor(),
                    StructProto.getDescriptor(),
                    TimestampProto.getDescriptor(),
                    TypeProto.getDescriptor(),
                    WrappersProto.getDescriptor(),
                    // the Google common types that schema registry serializers treat as known types and do not
                    // register as references either; the other files of google/type resolve like any import
                    CalendarPeriodProto.getDescriptor(),
                    ColorProto.getDescriptor(),
                    DateProto.getDescriptor(),
                    DateTimeProto.getDescriptor(),
                    DayOfWeekProto.getDescriptor(),
                    DecimalProto.getDescriptor(),
                    ExprProto.getDescriptor(),
                    FractionProto.getDescriptor(),
                    IntervalProto.getDescriptor(),
                    LatLngProto.getDescriptor(),
                    MoneyProto.getDescriptor(),
                    MonthProto.getDescriptor(),
                    PhoneNumberProto.getDescriptor(),
                    PostalAddressProto.getDescriptor(),
                    QuaternionProto.getDescriptor(),
                    TimeOfDayProto.getDescriptor())
            .collect(toImmutableMap(FileDescriptor::getName, identity()));

    private static final Map<String, Type> SCALAR_TYPES = ImmutableMap.<String, Type>builder()
            .put("double", Type.TYPE_DOUBLE)
            .put("float", Type.TYPE_FLOAT)
            .put("int32", Type.TYPE_INT32)
            .put("int64", Type.TYPE_INT64)
            .put("uint32", Type.TYPE_UINT32)
            .put("uint64", Type.TYPE_UINT64)
            .put("sint32", Type.TYPE_SINT32)
            .put("sint64", Type.TYPE_SINT64)
            .put("fixed32", Type.TYPE_FIXED32)
            .put("fixed64", Type.TYPE_FIXED64)
            .put("sfixed32", Type.TYPE_SFIXED32)
            .put("sfixed64", Type.TYPE_SFIXED64)
            .put("bool", Type.TYPE_BOOL)
            .put("string", Type.TYPE_STRING)
            .put("bytes", Type.TYPE_BYTES)
            .buildOrThrow();

    private final ImportResolver importResolver;
    private final Map<String, FileDescriptor> builtFiles = new HashMap<>();
    private final Deque<String> filesBeingBuilt = new ArrayDeque<>();

    private ProtoFileDescriptorBuilder(ImportResolver importResolver)
    {
        this.importResolver = requireNonNull(importResolver, "importResolver is null");
    }

    /**
     * Builds the descriptor of a file and, transitively, of everything it imports.
     *
     * @param fileName name recorded in the descriptor; imports of other files refer to them by such names
     * @param protoSource the {@code .proto} text
     * @param importResolver supplies the text of imported files that are not well-known types
     * @throws TrinoException with {@link ProtobufErrorCode#INVALID_PROTO_FILE} if any file fails to parse or link, or an import cannot be resolved
     */
    public static FileDescriptor buildFileDescriptor(String fileName, String protoSource, ImportResolver importResolver)
    {
        requireNonNull(fileName, "fileName is null");
        requireNonNull(protoSource, "protoSource is null");
        return new ProtoFileDescriptorBuilder(importResolver).buildFile(fileName, protoSource);
    }

    private FileDescriptor buildFile(String fileName, String protoSource)
    {
        if (filesBeingBuilt.contains(fileName)) {
            throw new TrinoException(INVALID_PROTO_FILE, format("Import cycle in protobuf schema: %s -> %s", String.join(" -> ", filesBeingBuilt.reversed()), fileName));
        }
        filesBeingBuilt.push(fileName);
        try {
            ProtoFileElement file = parse(fileName, protoSource);
            FileDescriptorProto.Builder proto = FileDescriptorProto.newBuilder();
            if (!fileName.isEmpty()) {
                proto.setName(fileName);
            }
            if (file.getSyntax() != null) {
                proto.setSyntax(file.getSyntax().toString());
            }
            if (file.getPackageName() != null) {
                proto.setPackage(file.getPackageName());
            }

            List<FileDescriptor> dependencies = new ArrayList<>();
            for (String importPath : file.getImports()) {
                proto.addDependency(importPath);
                dependencies.add(resolveImport(importPath, fileName));
            }
            for (String importPath : file.getPublicImports()) {
                proto.addPublicDependency(proto.getDependencyCount());
                proto.addDependency(importPath);
                dependencies.add(resolveImport(importPath, fileName));
            }
            for (String importPath : file.getWeakImports()) {
                proto.addWeakDependency(proto.getDependencyCount());
                proto.addDependency(importPath);
                dependencies.add(resolveImport(importPath, fileName));
            }

            boolean proto3 = file.getSyntax() == Syntax.PROTO_3;
            for (TypeElement type : file.getTypes()) {
                if (type instanceof MessageElement message) {
                    proto.addMessageType(toDescriptorProto(message, proto3, fileName));
                }
                else if (type instanceof EnumElement enumElement) {
                    proto.addEnumType(toEnumDescriptorProto(enumElement, fileName));
                }
            }

            try {
                FileDescriptor descriptor = FileDescriptor.buildFrom(proto.build(), dependencies.toArray(new FileDescriptor[0]));
                builtFiles.put(fileName, descriptor);
                return descriptor;
            }
            catch (DescriptorValidationException e) {
                throw new TrinoException(INVALID_PROTO_FILE, format("Invalid protobuf schema '%s': %s", fileName, e.getMessage()), e);
            }
        }
        finally {
            filesBeingBuilt.pop();
        }
    }

    private static ProtoFileElement parse(String fileName, String protoSource)
    {
        try {
            return ProtoParser.Companion.parse(Location.get(fileName), protoSource);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_PROTO_FILE, format("Failed to parse protobuf schema '%s': %s", fileName, e.getMessage()), e);
        }
    }

    private FileDescriptor resolveImport(String importPath, String importedBy)
    {
        FileDescriptor wellKnownFile = WELL_KNOWN_FILES.get(importPath);
        if (wellKnownFile != null) {
            return wellKnownFile;
        }
        FileDescriptor builtFile = builtFiles.get(importPath);
        if (builtFile != null) {
            return builtFile;
        }
        String source = importResolver.getProtoSource(importPath)
                .orElseThrow(() -> new TrinoException(INVALID_PROTO_FILE, format("Import '%s' of protobuf schema '%s' could not be resolved", importPath, importedBy)));
        return buildFile(importPath, source);
    }

    private static DescriptorProto toDescriptorProto(MessageElement message, boolean proto3, String fileName)
    {
        if (!message.getGroups().isEmpty()) {
            throw new TrinoException(INVALID_PROTO_FILE, format("Groups are not supported: message '%s' in protobuf schema '%s'", message.getName(), fileName));
        }
        DescriptorProto.Builder builder = DescriptorProto.newBuilder()
                .setName(message.getName());
        for (OptionElement option : message.getOptions()) {
            if (option.isParenthesized()) {
                continue;
            }
            switch (option.getName()) {
                // schema text generated from a descriptor, such as the text a schema registry
                // serializer registers, declares map entry messages explicitly
                case "map_entry" -> builder.getOptionsBuilder().setMapEntry(toBoolean(option));
                case "deprecated" -> builder.getOptionsBuilder().setDeprecated(toBoolean(option));
                default -> {
                    // other options do not affect decoding
                }
            }
        }
        checkReserved(message, fileName);
        List<DescriptorProto> mapEntries = new ArrayList<>();
        for (FieldElement field : message.getFields()) {
            builder.addField(toFieldDescriptorProto(field, proto3, OptionalInt.empty(), mapEntries));
        }
        for (OneOfElement oneOf : message.getOneOfs()) {
            if (!oneOf.getGroups().isEmpty()) {
                throw new TrinoException(INVALID_PROTO_FILE, format("Groups are not supported: oneof '%s' in message '%s' in protobuf schema '%s'", oneOf.getName(), message.getName(), fileName));
            }
            int oneofIndex = builder.getOneofDeclCount();
            builder.addOneofDecl(OneofDescriptorProto.newBuilder().setName(oneOf.getName()));
            for (FieldElement field : oneOf.getFields()) {
                builder.addField(toFieldDescriptorProto(field, proto3, OptionalInt.of(oneofIndex), mapEntries));
            }
        }
        // protoc represents a proto3 "optional" field as the only member of a synthetic oneof
        // named after the field; synthetic oneofs must follow all declared ones
        for (FieldDescriptorProto.Builder field : builder.getFieldBuilderList()) {
            if (field.getProto3Optional()) {
                field.setOneofIndex(builder.getOneofDeclCount());
                builder.addOneofDecl(OneofDescriptorProto.newBuilder().setName("_" + field.getName()));
            }
        }
        for (TypeElement nested : message.getNestedTypes()) {
            if (nested instanceof MessageElement nestedMessage) {
                builder.addNestedType(toDescriptorProto(nestedMessage, proto3, fileName));
            }
            else if (nested instanceof EnumElement nestedEnum) {
                builder.addEnumType(toEnumDescriptorProto(nestedEnum, fileName));
            }
        }
        // map entry types go last, so the position of a declared nested message among the
        // nested types matches its position in the source
        mapEntries.forEach(builder::addNestedType);
        if (builder.getOptions().getMapEntry() && !isMapEntryShape(builder)) {
            throw new TrinoException(INVALID_PROTO_FILE, format("Message '%s' in protobuf schema '%s' is declared as a map entry but does not consist of the fields key = 1 and value = 2", message.getName(), fileName));
        }
        return builder.build();
    }

    /**
     * The shape protoc gives the entry message of a map field, which is what decoding and encoding a map rely on.
     */
    private static boolean isMapEntryShape(DescriptorProto.Builder message)
    {
        if (message.getFieldCount() != 2 || message.getNestedTypeCount() != 0 || message.getEnumTypeCount() != 0 || message.getOneofDeclCount() != 0) {
            return false;
        }
        return isMapEntryField(message.getField(0), "key", 1) && isMapEntryField(message.getField(1), "value", 2);
    }

    private static boolean isMapEntryField(FieldDescriptorProto field, String name, int number)
    {
        return field.getName().equals(name) && field.getNumber() == number && field.getLabel() == Label.LABEL_OPTIONAL;
    }

    private static FieldDescriptorProto toFieldDescriptorProto(FieldElement field, boolean proto3, OptionalInt oneofIndex, List<DescriptorProto> mapEntries)
    {
        FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                .setName(field.getName())
                .setNumber(field.getTag());
        ProtoType type = ProtoType.get(field.getType());
        if (type.isMap()) {
            String entryName = toMapEntryName(field.getName());
            mapEntries.add(DescriptorProto.newBuilder()
                    .setName(entryName)
                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                    .addField(setType(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setLabel(Label.LABEL_OPTIONAL), requireNonNull(type.getKeyType(), "keyType is null")))
                    .addField(setType(FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setLabel(Label.LABEL_OPTIONAL), requireNonNull(type.getValueType(), "valueType is null")))
                    .build());
            return builder
                    .setType(Type.TYPE_MESSAGE)
                    .setTypeName(entryName)
                    .setLabel(Label.LABEL_REPEATED)
                    .build();
        }

        setType(builder, type);
        Field.Label label = field.getLabel();
        if (label == null || label == Field.Label.ONE_OF) {
            builder.setLabel(Label.LABEL_OPTIONAL);
        }
        else {
            switch (label) {
                case OPTIONAL -> {
                    builder.setLabel(Label.LABEL_OPTIONAL);
                    if (proto3) {
                        builder.setProto3Optional(true);
                    }
                }
                case REQUIRED -> builder.setLabel(Label.LABEL_REQUIRED);
                case REPEATED -> builder.setLabel(Label.LABEL_REPEATED);
                default -> throw new IllegalArgumentException("Unknown label: " + label);
            }
        }
        oneofIndex.ifPresent(builder::setOneofIndex);
        if (field.getDefaultValue() != null) {
            if (builder.getType() == Type.TYPE_BYTES) {
                // the parser unescapes the literal, while the descriptor expects a bytes default C-escaped
                builder.setDefaultValue(escapeBytes(toBytesDefault(field.getDefaultValue())));
            }
            else {
                builder.setDefaultValue(field.getDefaultValue());
            }
        }
        if (field.getJsonName() != null) {
            builder.setJsonName(field.getJsonName());
        }
        for (OptionElement option : field.getOptions()) {
            if (option.isParenthesized()) {
                continue;
            }
            switch (option.getName()) {
                case "packed" -> builder.getOptionsBuilder().setPacked(toBoolean(option));
                case "deprecated" -> builder.getOptionsBuilder().setDeprecated(toBoolean(option));
                default -> {
                    // other options do not affect decoding
                }
            }
        }
        return builder.build();
    }

    private static FieldDescriptorProto.Builder setType(FieldDescriptorProto.Builder builder, ProtoType type)
    {
        Type scalarType = SCALAR_TYPES.get(type.toString());
        if (scalarType != null) {
            return builder.setType(scalarType);
        }
        // leave the type unset: protobuf-java resolves the name and decides between message and enum
        return builder.setTypeName(type.toString());
    }

    /**
     * protoc rejects a field that uses a number or name the message reserves, so such a schema
     * is most likely a mistake and could not have been registered by a serializer.
     */
    private static void checkReserved(MessageElement message, String fileName)
    {
        List<Reserved> reserveds = Reserved.fromElements(message.getReserveds());
        Stream.concat(message.getFields().stream(), message.getOneOfs().stream().flatMap(oneOf -> oneOf.getFields().stream()))
                .forEach(field -> {
                    if (reserveds.stream().anyMatch(reserved -> reserved.matchesTag(field.getTag()))) {
                        throw new TrinoException(INVALID_PROTO_FILE, format("Field '%s' in message '%s' in protobuf schema '%s' uses reserved number %s", field.getName(), message.getName(), fileName, field.getTag()));
                    }
                    if (reserveds.stream().anyMatch(reserved -> reserved.matchesName(field.getName()))) {
                        throw new TrinoException(INVALID_PROTO_FILE, format("Field '%s' in message '%s' in protobuf schema '%s' uses a reserved name", field.getName(), message.getName(), fileName));
                    }
                });
    }

    private static void checkReserved(EnumElement enumElement, String fileName)
    {
        // the parser ends a range up to "max" at the largest field number, also in an enum, where
        // protoc ends it at the largest int32, so a larger value in such a range is not rejected
        List<Reserved> reserveds = Reserved.fromElements(enumElement.getReserveds());
        for (EnumConstantElement constant : enumElement.getConstants()) {
            if (reserveds.stream().anyMatch(reserved -> reserved.matchesTag(constant.getTag()))) {
                throw new TrinoException(INVALID_PROTO_FILE, format("Value '%s' in enum '%s' in protobuf schema '%s' uses reserved number %s", constant.getName(), enumElement.getName(), fileName, constant.getTag()));
            }
            if (reserveds.stream().anyMatch(reserved -> reserved.matchesName(constant.getName()))) {
                throw new TrinoException(INVALID_PROTO_FILE, format("Value '%s' in enum '%s' in protobuf schema '%s' uses a reserved name", constant.getName(), enumElement.getName(), fileName));
            }
        }
    }

    private static EnumDescriptorProto toEnumDescriptorProto(EnumElement enumElement, String fileName)
    {
        checkReserved(enumElement, fileName);
        EnumDescriptorProto.Builder builder = EnumDescriptorProto.newBuilder()
                .setName(enumElement.getName());
        for (EnumConstantElement constant : enumElement.getConstants()) {
            builder.addValue(EnumValueDescriptorProto.newBuilder()
                    .setName(constant.getName())
                    .setNumber(constant.getTag()));
        }
        for (OptionElement option : enumElement.getOptions()) {
            if (option.isParenthesized()) {
                continue;
            }
            switch (option.getName()) {
                case "allow_alias" -> builder.getOptionsBuilder().setAllowAlias(toBoolean(option));
                case "deprecated" -> builder.getOptionsBuilder().setDeprecated(toBoolean(option));
                default -> {
                    // other options do not affect decoding
                }
            }
        }
        return builder.build();
    }

    /**
     * The bytes of an unescaped bytes default. An octal or hex escape stands for one byte and is
     * unescaped to the character of that value, so such characters are taken as bytes; other text
     * is encoded as UTF-8, as {@code protoc} does.
     */
    private static byte[] toBytesDefault(String value)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(value.length());
        value.codePoints().forEach(codePoint -> {
            if (codePoint <= 0xFF) {
                bytes.write(codePoint);
            }
            else {
                bytes.writeBytes(Character.toString(codePoint).getBytes(UTF_8));
            }
        });
        return bytes.toByteArray();
    }

    private static boolean toBoolean(OptionElement option)
    {
        return Boolean.parseBoolean(String.valueOf(option.getValue()));
    }

    /**
     * The name protoc gives the synthetic entry message of a map field: the field name with
     * underscores removed and the following letters capitalized, followed by "Entry".
     */
    private static String toMapEntryName(String fieldName)
    {
        StringBuilder name = new StringBuilder(fieldName.length() + 5);
        boolean capitalizeNext = true;
        for (int i = 0; i < fieldName.length(); i++) {
            char character = fieldName.charAt(i);
            if (character == '_') {
                capitalizeNext = true;
            }
            else if (capitalizeNext) {
                name.append(Character.toUpperCase(character));
                capitalizeNext = false;
            }
            else {
                name.append(character);
            }
        }
        return name.append("Entry").toString();
    }
}
