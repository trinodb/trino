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

import com.google.common.io.Resources;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_GROUP;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class ProtobufUtils
{
    private ProtobufUtils()
    {}

    public static FileDescriptor getFileDescriptor(String protoFile)
            throws DescriptorValidationException
    {
        return getFileDescriptor(Optional.empty(), protoFile);
    }

    public static FileDescriptor getFileDescriptor(Optional<String> fileName, String protoFile)
            throws DescriptorValidationException
    {
        ProtoFileElement protoFileElement = ProtoParser.Companion.parse(Location.get(""), protoFile);
        return getFileDescriptor(fileName, protoFileElement);
    }

    public static FileDescriptor getFileDescriptor(Optional<String> fileName, ProtoFileElement protoFileElement)
            throws DescriptorValidationException
    {
        FileDescriptor[] dependencies = new FileDescriptor[protoFileElement.getImports().size()];
        Set<String> definedMessages = new HashSet<>();
        int index = 0;
        for (String importStatement : protoFileElement.getImports()) {
            try {
                FileDescriptor fileDescriptor = getFileDescriptor(Optional.of(importStatement), getProtoFile(importStatement));
                fileDescriptor.getMessageTypes().stream()
                        .map(Descriptor::getFullName)
                        .forEach(definedMessages::add);
                dependencies[index] = fileDescriptor;
                index++;
            }
            catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        FileDescriptorProto.Builder builder = FileDescriptorProto.newBuilder();

        if (protoFileElement.getSyntax() != null) {
            builder.setSyntax(protoFileElement.getSyntax().name());
        }

        fileName.ifPresent(builder::setName);
        builder.addAllDependency(protoFileElement.getImports());

        if (protoFileElement.getPackageName() != null) {
            builder.setPackage(protoFileElement.getPackageName());
        }

        for (TypeElement element : protoFileElement.getTypes()) {
            if (element instanceof MessageElement) {
                builder.addMessageType(processMessage((MessageElement) element, definedMessages));
                definedMessages.add(element.getName());
            }
            if (element instanceof EnumElement) {
                builder.addEnumType(processEnum((EnumElement) element));
            }
        }

        return FileDescriptor.buildFrom(builder.build(), dependencies);
    }

    public static String getProtoFile(String filePath)
            throws IOException
    {
        return Resources.toString(Resources.getResource(ProtobufUtils.class, "/" + filePath), UTF_8);
    }

    private static DescriptorProto processMessage(MessageElement message, Set<String> globallyDefinedMessages)
    {
        DescriptorProto.Builder builder = DescriptorProto.newBuilder();
        builder.setName(message.getName());
        Set<String> definedMessages = new HashSet<>(globallyDefinedMessages);
        for (TypeElement typeElement : message.getNestedTypes()) {
            if (typeElement instanceof EnumElement) {
                builder.addEnumType(processEnum((EnumElement) typeElement));
            }
            if (typeElement instanceof MessageElement) {
                builder.addNestedType(processMessage((MessageElement) typeElement, definedMessages));
                definedMessages.add(typeElement.getName());
            }
        }
        for (FieldElement field : message.getFields()) {
            ProtoType protoType = ProtoType.get(field.getType());
            FieldDescriptorProto.Builder fieldDescriptor = FieldDescriptorProto.newBuilder()
                    .setName(field.getName())
                    .setNumber(field.getTag());
            if (protoType.isMap()) {
                requireNonNull(protoType.getKeyType(), "keyType is null");
                requireNonNull(protoType.getValueType(), "valueType is null");
                builder.addNestedType(DescriptorProto.newBuilder()
                        //First Name to Upper case
                        .setName(getNameForMapField(field.getName()))
                        .setOptions(MessageOptions.newBuilder().setMapEntry(true).build())
                        .addField(
                                processType(
                                        FieldDescriptorProto.newBuilder()
                                                .setName("key")
                                                .setNumber(1)
                                                .setLabel(Label.LABEL_OPTIONAL),
                                        protoType.getKeyType(),
                                        definedMessages))
                        .addField(
                                processType(
                                        FieldDescriptorProto.newBuilder()
                                                .setName("value")
                                                .setNumber(2)
                                                .setLabel(Label.LABEL_OPTIONAL),
                                        protoType.getValueType(),
                                        definedMessages))
                                .build());
                // Handle for underscores and name
                fieldDescriptor.setType(TYPE_MESSAGE)
                        .setLabel(Label.LABEL_REPEATED)
                        .setTypeName(getNameForMapField(field.getName()));
            }
            else {
                processType(fieldDescriptor, protoType, definedMessages);
            }
            if (field.getLabel() != null && field.getLabel() != Field.Label.ONE_OF) {
                fieldDescriptor.setLabel(getLabel(field.getLabel()));
            }
            if (field.getDefaultValue() != null) {
                fieldDescriptor.setDefaultValue(field.getDefaultValue());
            }
            builder.addField(fieldDescriptor.build());
        }
        return builder.build();
    }

    private static EnumDescriptorProto processEnum(EnumElement enumElement)
    {
        EnumDescriptorProto.Builder enumBuilder = EnumDescriptorProto.newBuilder();
        enumBuilder.setName(enumElement.getName());
        for (EnumConstantElement enumConstant : enumElement.getConstants()) {
            enumBuilder.addValue(EnumValueDescriptorProto.newBuilder()
                    .setName(enumConstant.getName())
                    .setNumber(enumConstant.getTag())
                    .build());
        }
        return enumBuilder.build();
    }

    public static FieldDescriptorProto.Builder processType(FieldDescriptorProto.Builder builder, ProtoType type, Set<String> messageNames)
    {
        switch (type.getSimpleName()) {
            case "double" :
                return builder.setType(TYPE_DOUBLE);
            case "float" :
                return builder.setType(TYPE_FLOAT);
            case "int64" :
                return builder.setType(TYPE_INT64);
            case "uint64" :
                return builder.setType(TYPE_UINT64);
            case "int32" :
                return builder.setType(TYPE_INT32);
            case "fixed64" :
                return builder.setType(TYPE_FIXED64);
            case "fixed32" :
                return builder.setType(TYPE_FIXED32);
            case "bool" :
                return builder.setType(TYPE_BOOL);
            case "string" :
                return builder.setType(TYPE_STRING);
            case "group" :
                return builder.setType(TYPE_GROUP);
            case "bytes" :
                return builder.setType(TYPE_BYTES);
            case "uint32" :
                return builder.setType(TYPE_UINT32);
            case "sfixed32" :
                return builder.setType(TYPE_SFIXED32);
            case "sfixed64" :
                return builder.setType(TYPE_SFIXED64);
            case "sint32" :
                return builder.setType(TYPE_SINT32);
            case "sint64" :
                return builder.setType(TYPE_SINT64);
            default: {
                builder.setTypeName(type.toString());
                if (messageNames.contains(type.toString())) {
                    builder.setType(TYPE_MESSAGE);
                }
                else {
                    builder.setType(TYPE_ENUM);
                }
                return builder;
            }
        }
    }

    public static Label getLabel(Field.Label label)
    {
        switch (label) {
            case OPTIONAL:
                return Label.LABEL_OPTIONAL;
            case REPEATED:
                return Label.LABEL_REPEATED;
            case REQUIRED:
                return Label.LABEL_REQUIRED;
            default:
                throw new IllegalArgumentException("Unknown label");
        }
    }

    private static String getNameForMapField(String fieldName)
    {
        return LOWER_UNDERSCORE.to(UPPER_CAMEL, fieldName) + "Entry";
    }
}
