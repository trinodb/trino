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
package io.trino.plugin.pulsar.decoder.protobufnative;

@SuppressWarnings("deprecation")
public final class TestMsg
{
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_proto_SubMessage_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_proto_SubMessage_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_proto_SubMessage_NestedMessage_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_proto_SubMessage_NestedMessage_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_proto_TestMessage_descriptor;
    private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internal_static_proto_TestMessage_fieldAccessorTable;
    private static final com.google.protobuf.Descriptors.Descriptor
            internal_static_proto_TestMessage_MapFieldEntry_descriptor;
    private static com.google.protobuf.Descriptors.FileDescriptor
            descriptor;

    static {
        String[] descriptorData = {
                "\n\rTestMsg.proto\022\005proto\"\214\001\n\nSubMessage\022\013\n" +
                        "\003foo\030\001 \001(\t\022\013\n\003bar\030\002 \001(\001\0226\n\rnestedMessage" +
                        "\030\003 \001(\0132\037.proto.SubMessage.NestedMessage\032" +
                        ",\n\rNestedMessage\022\r\n\005title\030\001 \001(\t\022\014\n\004urls\030" +
                        "\002 \003(\t\"\216\004\n\013TestMessage\022\023\n\013stringField\030\001 \001" +
                        "(\t\022\023\n\013doubleField\030\002 \001(\001\022\022\n\nfloatField\030\003 " +
                        "\001(\002\022\022\n\nint32Field\030\004 \001(\005\022\022\n\nint64Field\030\005 " +
                        "\001(\003\022\023\n\013uint32Field\030\006 \001(\r\022\023\n\013uint64Field\030" +
                        "\007 \001(\004\022\023\n\013sint32Field\030\010 \001(\021\022\023\n\013sint64Fiel" +
                        "d\030\t \001(\022\022\024\n\014fixed32Field\030\n \001(\007\022\024\n\014fixed64" +
                        "Field\030\013 \001(\006\022\025\n\rsfixed32Field\030\014 \001(\017\022\025\n\rsf" +
                        "ixed64Field\030\r \001(\020\022\021\n\tboolField\030\016 \001(\010\022\022\n\n" +
                        "bytesField\030\017 \001(\014\022!\n\010testEnum\030\020 \001(\0162\017.pro" +
                        "to.TestEnum\022%\n\nsubMessage\030\021 \001(\0132\021.proto." +
                        "SubMessage\022\025\n\rrepeatedField\030\022 \003(\t\0222\n\010map" +
                        "Field\030\023 \003(\0132 .proto.TestMessage.MapField" +
                        "Entry\032/\n\rMapFieldEntry\022\013\n\003key\030\001 \001(\t\022\r\n\005v" +
                        "alue\030\002 \001(\001:\0028\001*$\n\010TestEnum\022\n\n\006SHARED\020\000\022\014" +
                        "\n\010FAILOVER\020\001B>\n3org.apache.pulsar.sql.pr" +
                        "esto.decoder.protobufnativeB\007TestMsgb\006pr" +
                        "oto3"
        };
        com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
                new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner()
                {
                    @Override
                    public com.google.protobuf.ExtensionRegistry assignDescriptors(com.google.protobuf.Descriptors.FileDescriptor root)
                    {
                        descriptor = root;
                        return null;
                    }
                };
        com.google.protobuf.Descriptors.FileDescriptor
                .internalBuildGeneratedFileFrom(descriptorData,
                        new com.google.protobuf.Descriptors.FileDescriptor[]{
                        }, assigner);
        internal_static_proto_SubMessage_descriptor =
                getDescriptor().getMessageTypes().get(0);
        internal_static_proto_SubMessage_fieldAccessorTable = new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                internal_static_proto_SubMessage_descriptor, new String[] {"Foo", "Bar", "NestedMessage"});
        internal_static_proto_SubMessage_NestedMessage_descriptor =
                internal_static_proto_SubMessage_descriptor.getNestedTypes().get(0);
        internal_static_proto_SubMessage_NestedMessage_fieldAccessorTable = new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                internal_static_proto_SubMessage_NestedMessage_descriptor, new String[] {"Title", "Urls"});
        internal_static_proto_TestMessage_descriptor =
                getDescriptor().getMessageTypes().get(1);
        internal_static_proto_TestMessage_fieldAccessorTable = new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
                internal_static_proto_TestMessage_descriptor, new String[] {"StringField", "DoubleField", "FloatField", "Int32Field", "Int64Field", "Uint32Field", "Uint64Field", "Sint32Field", "Sint64Field", "Fixed32Field", "Fixed64Field", "Sfixed32Field", "Sfixed64Field", "BoolField", "BytesField", "TestEnum", "SubMessage", "RepeatedField", "MapField"});
        internal_static_proto_TestMessage_MapFieldEntry_descriptor =
                internal_static_proto_TestMessage_descriptor.getNestedTypes().get(0);
    }

    private TestMsg()
    {
    }

    public static void registerAllExtensions(
            com.google.protobuf.ExtensionRegistryLite registry)
    {
    }

    public static void registerAllExtensions(
            com.google.protobuf.ExtensionRegistry registry)
    {
        registerAllExtensions(
                (com.google.protobuf.ExtensionRegistryLite) registry);
    }

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor()
    {
        return descriptor;
    }

    /**
     * Protobuf enum {@code proto.TestEnum}
     */
    public enum TestEnum
            implements com.google.protobuf.ProtocolMessageEnum
    {
        /**
         * <code>SHARED = 0;</code>
         */
        SHARED(0),
        /**
         * <code>FAILOVER = 1;</code>
         */
        FAILOVER(1),
        UNRECOGNIZED(-1),;

        /**
         * <code>SHARED = 0;</code>
         */
        public static final int SHARED_VALUE = 0;
        /**
         * <code>FAILOVER = 1;</code>
         */
        public static final int FAILOVER_VALUE = 1;
        private static final com.google.protobuf.Internal.EnumLiteMap<
                TestEnum> internalValueMap =
                new com.google.protobuf.Internal.EnumLiteMap<TestEnum>()
                {
                    @Override
                    public TestEnum findValueByNumber(int number)
                    {
                        return TestEnum.forNumber(number);
                    }
                };
        private static final TestEnum[] VALUES = values();
        private final int value;

        TestEnum(int value)
        {
            this.value = value;
        }

        /**
         * @deprecated Use {@link #forNumber(int)} instead.
         */
        @Deprecated
        public static TestEnum valueOf(int value)
        {
            return forNumber(value);
        }

        public static TestEnum forNumber(int value)
        {
            switch (value) {
                case 0:
                    return SHARED;
                case 1:
                    return FAILOVER;
                default:
                    return null;
            }
        }

        public static com.google.protobuf.Internal.EnumLiteMap<TestEnum> internalGetValueMap()
        {
            return internalValueMap;
        }

        public static com.google.protobuf.Descriptors.EnumDescriptor getDescriptor()
        {
            return TestMsg.getDescriptor().getEnumTypes().get(0);
        }

        public static TestEnum valueOf(
                com.google.protobuf.Descriptors.EnumValueDescriptor desc)
        {
            if (desc.getType() != getDescriptor()) {
                throw new IllegalArgumentException(
                        "EnumValueDescriptor is not for this type.");
            }
            if (desc.getIndex() == -1) {
                return UNRECOGNIZED;
            }
            return VALUES[desc.getIndex()];
        }

        @Override
        public final int getNumber()
        {
            if (this == UNRECOGNIZED) {
                throw new IllegalArgumentException(
                        "Can't get the number of an unknown enum value.");
            }
            return value;
        }

        @Override
        public final com.google.protobuf.Descriptors.EnumValueDescriptor getValueDescriptor()
        {
            return getDescriptor().getValues().get(ordinal());
        }

        @Override
        public final com.google.protobuf.Descriptors.EnumDescriptor getDescriptorForType()
        {
            return getDescriptor();
        }

        // @@protoc_insertion_point(enum_scope:proto.TestEnum)
    }

    public interface SubMessageOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:proto.SubMessage)
            com.google.protobuf.MessageOrBuilder
    {
        /**
         * <code>string foo = 1;</code>
         */
        String getFoo();

        /**
         * <code>string foo = 1;</code>
         */
        com.google.protobuf.ByteString getFooBytes();

        /**
         * <code>double bar = 2;</code>
         */
        double getBar();

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        boolean hasNestedMessage();

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        SubMessage.NestedMessage getNestedMessage();

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        SubMessage.NestedMessageOrBuilder getNestedMessageOrBuilder();
    }

    public interface TestMessageOrBuilder
            extends
            // @@protoc_insertion_point(interface_extends:proto.TestMessage)
            com.google.protobuf.MessageOrBuilder
    {
        /**
         * <code>string stringField = 1;</code>
         */
        String getStringField();

        /**
         * <code>string stringField = 1;</code>
         */
        com.google.protobuf.ByteString getStringFieldBytes();

        /**
         * <code>double doubleField = 2;</code>
         */
        double getDoubleField();

        /**
         * <code>float floatField = 3;</code>
         */
        float getFloatField();

        /**
         * <code>int32 int32Field = 4;</code>
         */
        int getInt32Field();

        /**
         * <code>int64 int64Field = 5;</code>
         */
        long getInt64Field();

        /**
         * <code>uint32 uint32Field = 6;</code>
         */
        int getUint32Field();

        /**
         * <code>uint64 uint64Field = 7;</code>
         */
        long getUint64Field();

        /**
         * <code>sint32 sint32Field = 8;</code>
         */
        int getSint32Field();

        /**
         * <code>sint64 sint64Field = 9;</code>
         */
        long getSint64Field();

        /**
         * <code>fixed32 fixed32Field = 10;</code>
         */
        int getFixed32Field();

        /**
         * <code>fixed64 fixed64Field = 11;</code>
         */
        long getFixed64Field();

        /**
         * <code>sfixed32 sfixed32Field = 12;</code>
         */
        int getSfixed32Field();

        /**
         * <code>sfixed64 sfixed64Field = 13;</code>
         */
        long getSfixed64Field();

        /**
         * <code>bool boolField = 14;</code>
         */
        boolean getBoolField();

        /**
         * <code>bytes bytesField = 15;</code>
         */
        com.google.protobuf.ByteString getBytesField();

        /**
         * <code>.proto.TestEnum testEnum = 16;</code>
         */
        int getTestEnumValue();

        /**
         * <code>.proto.TestEnum testEnum = 16;</code>
         */
        TestEnum getTestEnum();

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        boolean hasSubMessage();

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        SubMessage getSubMessage();

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        SubMessageOrBuilder getSubMessageOrBuilder();

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        java.util.List<String> getRepeatedFieldList();

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        int getRepeatedFieldCount();

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        String getRepeatedField(int index);

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        com.google.protobuf.ByteString getRepeatedFieldBytes(int index);

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        int getMapFieldCount();

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        boolean containsMapField(
                String key);

        /**
         * Use {@link #getMapFieldMap()} instead.
         */
        @Deprecated
        java.util.Map<String, Double> getMapField();

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        java.util.Map<String, Double> getMapFieldMap();

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */

        double getMapFieldOrDefault(
                String key,
                double defaultValue);

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */

        double getMapFieldOrThrow(
                String key);
    }

    /**
     * Protobuf type {@code proto.SubMessage}
     */
    public static final class SubMessage
            extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:proto.SubMessage)
            SubMessageOrBuilder
    {
        public static final int FOO_FIELD_NUMBER = 1;
        public static final int BAR_FIELD_NUMBER = 2;
        public static final int NESTEDMESSAGE_FIELD_NUMBER = 3;
        private static final long serialVersionUID = 0L;
        // @@protoc_insertion_point(class_scope:proto.SubMessage)
        private static final SubMessage DEFAULT_INSTANCE;
        private static final com.google.protobuf.Parser<SubMessage>
                PARSER = new com.google.protobuf.AbstractParser<SubMessage>()
                {
                    @Override
                    public SubMessage parsePartialFrom(com.google.protobuf.CodedInputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException
                    {
                        return new SubMessage(input, extensionRegistry);
                    }
                };

        static {
            DEFAULT_INSTANCE = new SubMessage();
        }

        private volatile Object foo;
        private double bar;
        private NestedMessage nestedMessage;
        private byte memoizedIsInitialized = -1;

        // Use SubMessage.newBuilder() to construct.
        private SubMessage(com.google.protobuf.GeneratedMessageV3.Builder<?> builder)
        {
            super(builder);
        }

        private SubMessage()
        {
            foo = "";
            bar = 0D;
        }

        private SubMessage(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            this();
            if (extensionRegistry == null) {
                throw new NullPointerException();
            }
            com.google.protobuf.UnknownFieldSet.Builder unknownFields =
                    com.google.protobuf.UnknownFieldSet.newBuilder();
            try {
                boolean done = false;
                while (!done) {
                    int tag = input.readTag();
                    switch (tag) {
                        case 0:
                            done = true;
                            break;
                        case 10: {
                            String s = input.readStringRequireUtf8();

                            foo = s;
                            break;
                        }
                        case 17: {
                            bar = input.readDouble();
                            break;
                        }
                        case 26: {
                            NestedMessage.Builder subBuilder = null;
                            if (nestedMessage != null) {
                                subBuilder = nestedMessage.toBuilder();
                            }
                            nestedMessage = input.readMessage(NestedMessage.parser(), extensionRegistry);
                            if (subBuilder != null) {
                                subBuilder.mergeFrom(nestedMessage);
                                nestedMessage = subBuilder.buildPartial();
                            }

                            break;
                        }
                        default: {
                            if (!parseUnknownFieldProto3(
                                    input, unknownFields, extensionRegistry, tag)) {
                                done = true;
                            }
                            break;
                        }
                    }
                }
            }
            catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage(this);
            }
            catch (java.io.IOException e) {
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        e).setUnfinishedMessage(this);
            }
            finally {
                this.unknownFields = unknownFields.build();
                makeExtensionsImmutable();
            }
        }

        public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
        {
            return TestMsg.internal_static_proto_SubMessage_descriptor;
        }

        public static SubMessage parseFrom(
                java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static SubMessage parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static SubMessage parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static SubMessage parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static SubMessage parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static SubMessage parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static SubMessage parseFrom(java.io.InputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input);
        }

        public static SubMessage parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input, extensionRegistry);
        }

        public static SubMessage parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseDelimitedWithIOException(PARSER, input);
        }

        public static SubMessage parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
        }

        public static SubMessage parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input);
        }

        public static SubMessage parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input, extensionRegistry);
        }

        public static Builder newBuilder()
        {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(SubMessage prototype)
        {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        public static SubMessage getDefaultInstance()
        {
            return DEFAULT_INSTANCE;
        }

        public static com.google.protobuf.Parser<SubMessage> parser()
        {
            return PARSER;
        }

        @Override
        public com.google.protobuf.UnknownFieldSet getUnknownFields()
        {
            return unknownFields;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable()
        {
            return TestMsg.internal_static_proto_SubMessage_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            SubMessage.class, Builder.class);
        }

        /**
         * <code>string foo = 1;</code>
         */
        @Override
        public String getFoo()
        {
            Object ref = foo;
            if (ref instanceof String) {
                return (String) ref;
            }
            else {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                foo = s;
                return s;
            }
        }

        /**
         * <code>string foo = 1;</code>
         */
        @Override
        public com.google.protobuf.ByteString getFooBytes()
        {
            Object ref = foo;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (String) ref);
                foo = b;
                return b;
            }
            else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <code>double bar = 2;</code>
         */
        @Override
        public double getBar()
        {
            return bar;
        }

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        @Override
        public boolean hasNestedMessage()
        {
            return nestedMessage != null;
        }

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        @Override
        public NestedMessage getNestedMessage()
        {
            return nestedMessage == null ? NestedMessage.getDefaultInstance() : nestedMessage;
        }

        /**
         * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
         */
        @Override
        public NestedMessageOrBuilder getNestedMessageOrBuilder()
        {
            return getNestedMessage();
        }

        @Override
        public boolean isInitialized()
        {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException
        {
            if (!getFooBytes().isEmpty()) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 1, foo);
            }
            if (bar != 0D) {
                output.writeDouble(2, bar);
            }
            if (nestedMessage != null) {
                output.writeMessage(3, getNestedMessage());
            }
            unknownFields.writeTo(output);
        }

        @Override
        public int getSerializedSize()
        {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            if (!getFooBytes().isEmpty()) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, foo);
            }
            if (bar != 0D) {
                size += com.google.protobuf.CodedOutputStream
                        .computeDoubleSize(2, bar);
            }
            if (nestedMessage != null) {
                size += com.google.protobuf.CodedOutputStream
                        .computeMessageSize(3, getNestedMessage());
            }
            size += unknownFields.getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj)
        {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof SubMessage other)) {
                return super.equals(obj);
            }

            boolean result = true;
            result = result && getFoo()
                    .equals(other.getFoo());
            result = result && (
                    Double.doubleToLongBits(getBar())
                            == Double.doubleToLongBits(
                            other.getBar()));
            result = result && (hasNestedMessage() == other.hasNestedMessage());
            if (hasNestedMessage()) {
                result = result && getNestedMessage()
                        .equals(other.getNestedMessage());
            }
            result = result && unknownFields.equals(other.unknownFields);
            return result;
        }

        @Override
        public int hashCode()
        {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            hash = (37 * hash) + FOO_FIELD_NUMBER;
            hash = (53 * hash) + getFoo().hashCode();
            hash = (37 * hash) + BAR_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    Double.doubleToLongBits(getBar()));
            if (hasNestedMessage()) {
                hash = (37 * hash) + NESTEDMESSAGE_FIELD_NUMBER;
                hash = (53 * hash) + getNestedMessage().hashCode();
            }
            hash = (29 * hash) + unknownFields.hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        @Override
        public Builder newBuilderForType()
        {
            return newBuilder();
        }

        @Override
        public Builder toBuilder()
        {
            return this == DEFAULT_INSTANCE
                    ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(
                BuilderParent parent)
        {
            Builder builder = new Builder(parent);
            return builder;
        }

        @Override
        public com.google.protobuf.Parser<SubMessage> getParserForType()
        {
            return PARSER;
        }

        @Override
        public SubMessage getDefaultInstanceForType()
        {
            return DEFAULT_INSTANCE;
        }

        public interface NestedMessageOrBuilder
                extends
                // @@protoc_insertion_point(interface_extends:proto.SubMessage.NestedMessage)
                com.google.protobuf.MessageOrBuilder
        {
            /**
             * <code>string title = 1;</code>
             */
            String getTitle();

            /**
             * <code>string title = 1;</code>
             */
            com.google.protobuf.ByteString getTitleBytes();

            /**
             * <code>repeated string urls = 2;</code>
             */
            java.util.List<String> getUrlsList();

            /**
             * <code>repeated string urls = 2;</code>
             */
            int getUrlsCount();

            /**
             * <code>repeated string urls = 2;</code>
             */
            String getUrls(int index);

            /**
             * <code>repeated string urls = 2;</code>
             */
            com.google.protobuf.ByteString getUrlsBytes(int index);
        }

        /**
         * Protobuf type {@code proto.SubMessage.NestedMessage}
         */
        public static final class NestedMessage
                extends com.google.protobuf.GeneratedMessageV3
                implements
                // @@protoc_insertion_point(message_implements:proto.SubMessage.NestedMessage)
                NestedMessageOrBuilder
        {
            public static final int TITLE_FIELD_NUMBER = 1;
            public static final int URLS_FIELD_NUMBER = 2;
            private static final long serialVersionUID = 0L;
            // @@protoc_insertion_point(class_scope:proto.SubMessage.NestedMessage)
            private static final NestedMessage DEFAULT_INSTANCE;
            private static final com.google.protobuf.Parser<NestedMessage>
                    PARSER = new com.google.protobuf.AbstractParser<NestedMessage>()
                    {
                        @Override
                        public NestedMessage parsePartialFrom(com.google.protobuf.CodedInputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                                throws com.google.protobuf.InvalidProtocolBufferException
                        {
                            return new NestedMessage(input, extensionRegistry);
                        }
                    };

            static {
                DEFAULT_INSTANCE = new NestedMessage();
            }

            private volatile Object title;
            private com.google.protobuf.LazyStringList urls;
            private byte memoizedIsInitialized = -1;

            // Use NestedMessage.newBuilder() to construct.
            private NestedMessage(com.google.protobuf.GeneratedMessageV3.Builder<?> builder)
            {
                super(builder);
            }

            private NestedMessage()
            {
                title = "";
                urls = com.google.protobuf.LazyStringArrayList.EMPTY;
            }

            private NestedMessage(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                this();
                if (extensionRegistry == null) {
                    throw new NullPointerException();
                }
                int mutableBitField0 = 0;
                com.google.protobuf.UnknownFieldSet.Builder unknownFields =
                        com.google.protobuf.UnknownFieldSet.newBuilder();
                try {
                    boolean done = false;
                    while (!done) {
                        int tag = input.readTag();
                        switch (tag) {
                            case 0:
                                done = true;
                                break;
                            case 10: {
                                String s = input.readStringRequireUtf8();

                                title = s;
                                break;
                            }
                            case 18: {
                                String s = input.readStringRequireUtf8();
                                if (!((mutableBitField0 & 0x00000002) == 0x00000002)) {
                                    urls = new com.google.protobuf.LazyStringArrayList();
                                    mutableBitField0 |= 0x00000002;
                                }
                                urls.add(s);
                                break;
                            }
                            default: {
                                if (!parseUnknownFieldProto3(
                                        input, unknownFields, extensionRegistry, tag)) {
                                    done = true;
                                }
                                break;
                            }
                        }
                    }
                }
                catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw e.setUnfinishedMessage(this);
                }
                catch (java.io.IOException e) {
                    throw new com.google.protobuf.InvalidProtocolBufferException(
                            e).setUnfinishedMessage(this);
                }
                finally {
                    if (((mutableBitField0 & 0x00000002) == 0x00000002)) {
                        urls = urls.getUnmodifiableView();
                    }
                    this.unknownFields = unknownFields.build();
                    makeExtensionsImmutable();
                }
            }

            public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
            {
                return TestMsg.internal_static_proto_SubMessage_NestedMessage_descriptor;
            }

            public static NestedMessage parseFrom(
                    java.nio.ByteBuffer data)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data);
            }

            public static NestedMessage parseFrom(
                    java.nio.ByteBuffer data,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data, extensionRegistry);
            }

            public static NestedMessage parseFrom(
                    com.google.protobuf.ByteString data)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data);
            }

            public static NestedMessage parseFrom(
                    com.google.protobuf.ByteString data,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data, extensionRegistry);
            }

            public static NestedMessage parseFrom(byte[] data)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data);
            }

            public static NestedMessage parseFrom(
                    byte[] data,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws com.google.protobuf.InvalidProtocolBufferException
            {
                return PARSER.parseFrom(data, extensionRegistry);
            }

            public static NestedMessage parseFrom(java.io.InputStream input)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseWithIOException(PARSER, input);
            }

            public static NestedMessage parseFrom(
                    java.io.InputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseWithIOException(PARSER, input, extensionRegistry);
            }

            public static NestedMessage parseDelimitedFrom(java.io.InputStream input)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseDelimitedWithIOException(PARSER, input);
            }

            public static NestedMessage parseDelimitedFrom(
                    java.io.InputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
            }

            public static NestedMessage parseFrom(
                    com.google.protobuf.CodedInputStream input)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseWithIOException(PARSER, input);
            }

            public static NestedMessage parseFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException
            {
                return com.google.protobuf.GeneratedMessageV3
                        .parseWithIOException(PARSER, input, extensionRegistry);
            }

            public static Builder newBuilder()
            {
                return DEFAULT_INSTANCE.toBuilder();
            }

            public static Builder newBuilder(NestedMessage prototype)
            {
                return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
            }

            public static NestedMessage getDefaultInstance()
            {
                return DEFAULT_INSTANCE;
            }

            public static com.google.protobuf.Parser<NestedMessage> parser()
            {
                return PARSER;
            }

            @Override
            public com.google.protobuf.UnknownFieldSet getUnknownFields()
            {
                return unknownFields;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable()
            {
                return TestMsg.internal_static_proto_SubMessage_NestedMessage_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                NestedMessage.class, Builder.class);
            }

            /**
             * <code>string title = 1;</code>
             */
            @Override
            public String getTitle()
            {
                Object ref = title;
                if (ref instanceof String) {
                    return (String) ref;
                }
                else {
                    com.google.protobuf.ByteString bs =
                            (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    title = s;
                    return s;
                }
            }

            /**
             * <code>string title = 1;</code>
             */
            @Override
            public com.google.protobuf.ByteString getTitleBytes()
            {
                Object ref = title;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    title = b;
                    return b;
                }
                else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>repeated string urls = 2;</code>
             */
            @Override
            public com.google.protobuf.ProtocolStringList getUrlsList()
            {
                return urls;
            }

            /**
             * <code>repeated string urls = 2;</code>
             */
            @Override
            public int getUrlsCount()
            {
                return urls.size();
            }

            /**
             * <code>repeated string urls = 2;</code>
             */
            @Override
            public String getUrls(int index)
            {
                return urls.get(index);
            }

            /**
             * <code>repeated string urls = 2;</code>
             */
            @Override
            public com.google.protobuf.ByteString getUrlsBytes(int index)
            {
                return urls.getByteString(index);
            }

            @Override
            public boolean isInitialized()
            {
                byte isInitialized = memoizedIsInitialized;
                if (isInitialized == 1) {
                    return true;
                }
                if (isInitialized == 0) {
                    return false;
                }

                memoizedIsInitialized = 1;
                return true;
            }

            @Override
            public void writeTo(com.google.protobuf.CodedOutputStream output)
                    throws java.io.IOException
            {
                if (!getTitleBytes().isEmpty()) {
                    com.google.protobuf.GeneratedMessageV3.writeString(output, 1, title);
                }
                for (int i = 0; i < urls.size(); i++) {
                    com.google.protobuf.GeneratedMessageV3.writeString(output, 2, urls.getRaw(i));
                }
                unknownFields.writeTo(output);
            }

            @Override
            public int getSerializedSize()
            {
                int size = memoizedSize;
                if (size != -1) {
                    return size;
                }

                size = 0;
                if (!getTitleBytes().isEmpty()) {
                    size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, title);
                }
                {
                    int dataSize = 0;
                    for (int i = 0; i < urls.size(); i++) {
                        dataSize += computeStringSizeNoTag(urls.getRaw(i));
                    }
                    size += dataSize;
                    size += getUrlsList().size();
                }
                size += unknownFields.getSerializedSize();
                memoizedSize = size;
                return size;
            }

            @Override
            public boolean equals(final Object obj)
            {
                if (obj == this) {
                    return true;
                }
                if (!(obj instanceof NestedMessage other)) {
                    return super.equals(obj);
                }

                boolean result = true;
                result = result && getTitle()
                        .equals(other.getTitle());
                result = result && getUrlsList()
                        .equals(other.getUrlsList());
                result = result && unknownFields.equals(other.unknownFields);
                return result;
            }

            @Override
            public int hashCode()
            {
                if (memoizedHashCode != 0) {
                    return memoizedHashCode;
                }
                int hash = 41;
                hash = (19 * hash) + getDescriptor().hashCode();
                hash = (37 * hash) + TITLE_FIELD_NUMBER;
                hash = (53 * hash) + getTitle().hashCode();
                if (getUrlsCount() > 0) {
                    hash = (37 * hash) + URLS_FIELD_NUMBER;
                    hash = (53 * hash) + getUrlsList().hashCode();
                }
                hash = (29 * hash) + unknownFields.hashCode();
                memoizedHashCode = hash;
                return hash;
            }

            @Override
            public Builder newBuilderForType()
            {
                return newBuilder();
            }

            @Override
            public Builder toBuilder()
            {
                return this == DEFAULT_INSTANCE
                        ? new Builder() : new Builder().mergeFrom(this);
            }

            @Override
            protected Builder newBuilderForType(
                    BuilderParent parent)
            {
                Builder builder = new Builder(parent);
                return builder;
            }

            @Override
            public com.google.protobuf.Parser<NestedMessage> getParserForType()
            {
                return PARSER;
            }

            @Override
            public NestedMessage getDefaultInstanceForType()
            {
                return DEFAULT_INSTANCE;
            }

            /**
             * Protobuf type {@code proto.SubMessage.NestedMessage}
             */
            public static final class Builder
                    extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                    implements
                    // @@protoc_insertion_point(builder_implements:proto.SubMessage.NestedMessage)
                    NestedMessageOrBuilder
            {
                private int bitField0;
                private Object title = "";
                private com.google.protobuf.LazyStringList urls = com.google.protobuf.LazyStringArrayList.EMPTY;

                // Construct using io.trino.plugin.pulsar.decoder.protobufnative.TestMsg.SubMessage.NestedMessage.newBuilder()
                private Builder()
                {
                    maybeForceBuilderInitialization();
                }

                private Builder(
                        BuilderParent parent)
                {
                    super(parent);
                    maybeForceBuilderInitialization();
                }

                public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
                {
                    return TestMsg.internal_static_proto_SubMessage_NestedMessage_descriptor;
                }

                @Override
                protected FieldAccessorTable internalGetFieldAccessorTable()
                {
                    return TestMsg.internal_static_proto_SubMessage_NestedMessage_fieldAccessorTable
                            .ensureFieldAccessorsInitialized(
                                    NestedMessage.class, Builder.class);
                }

                private void maybeForceBuilderInitialization()
                {
                }

                @Override
                public Builder clear()
                {
                    super.clear();
                    title = "";

                    urls = com.google.protobuf.LazyStringArrayList.EMPTY;
                    bitField0 = (bitField0 & ~0x00000002);
                    return this;
                }

                @Override
                public com.google.protobuf.Descriptors.Descriptor getDescriptorForType()
                {
                    return TestMsg.internal_static_proto_SubMessage_NestedMessage_descriptor;
                }

                @Override
                public NestedMessage getDefaultInstanceForType()
                {
                    return NestedMessage.getDefaultInstance();
                }

                @Override
                public NestedMessage build()
                {
                    NestedMessage result = buildPartial();
                    if (!result.isInitialized()) {
                        throw newUninitializedMessageException(result);
                    }
                    return result;
                }

                @Override
                public NestedMessage buildPartial()
                {
                    NestedMessage result = new NestedMessage(this);
                    result.title = title;
                    if (((bitField0 & 0x00000002) == 0x00000002)) {
                        urls = urls.getUnmodifiableView();
                        bitField0 = (bitField0 & ~0x00000002);
                    }
                    result.urls = urls;
                    onBuilt();
                    return result;
                }

                @Override
                public Builder clone()
                {
                    return super.clone();
                }

                @Override
                public Builder setField(
                        com.google.protobuf.Descriptors.FieldDescriptor field,
                        Object value)
                {
                    return super.setField(field, value);
                }

                @Override
                public Builder clearField(
                        com.google.protobuf.Descriptors.FieldDescriptor field)
                {
                    return super.clearField(field);
                }

                @Override
                public Builder clearOneof(
                        com.google.protobuf.Descriptors.OneofDescriptor oneof)
                {
                    return super.clearOneof(oneof);
                }

                @Override
                public Builder setRepeatedField(
                        com.google.protobuf.Descriptors.FieldDescriptor field,
                        int index, Object value)
                {
                    return super.setRepeatedField(field, index, value);
                }

                @Override
                public Builder addRepeatedField(
                        com.google.protobuf.Descriptors.FieldDescriptor field,
                        Object value)
                {
                    return super.addRepeatedField(field, value);
                }

                @Override
                public Builder mergeFrom(com.google.protobuf.Message other)
                {
                    if (other instanceof NestedMessage) {
                        return mergeFrom((NestedMessage) other);
                    }
                    else {
                        super.mergeFrom(other);
                        return this;
                    }
                }

                public Builder mergeFrom(NestedMessage other)
                {
                    if (other == NestedMessage.getDefaultInstance()) {
                        return this;
                    }
                    if (!other.getTitle().isEmpty()) {
                        title = other.title;
                        onChanged();
                    }
                    if (!other.urls.isEmpty()) {
                        if (urls.isEmpty()) {
                            urls = other.urls;
                            bitField0 = (bitField0 & ~0x00000002);
                        }
                        else {
                            ensureUrlsIsMutable();
                            urls.addAll(other.urls);
                        }
                        onChanged();
                    }
                    this.mergeUnknownFields(other.unknownFields);
                    onChanged();
                    return this;
                }

                @Override
                public boolean isInitialized()
                {
                    return true;
                }

                @Override
                public Builder mergeFrom(
                        com.google.protobuf.CodedInputStream input,
                        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                        throws java.io.IOException
                {
                    NestedMessage parsedMessage = null;
                    try {
                        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
                    }
                    catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        parsedMessage = (NestedMessage) e.getUnfinishedMessage();
                        throw e.unwrapIOException();
                    }
                    finally {
                        if (parsedMessage != null) {
                            mergeFrom(parsedMessage);
                        }
                    }
                    return this;
                }

                /**
                 * <code>string title = 1;</code>
                 */
                @Override
                public String getTitle()
                {
                    Object ref = title;
                    if (!(ref instanceof String)) {
                        com.google.protobuf.ByteString bs =
                                (com.google.protobuf.ByteString) ref;
                        String s = bs.toStringUtf8();
                        title = s;
                        return s;
                    }
                    else {
                        return (String) ref;
                    }
                }

                /**
                 * <code>string title = 1;</code>
                 */
                public Builder setTitle(
                        String value)
                {
                    if (value == null) {
                        throw new NullPointerException();
                    }

                    title = value;
                    onChanged();
                    return this;
                }

                /**
                 * <code>string title = 1;</code>
                 */
                @Override
                public com.google.protobuf.ByteString getTitleBytes()
                {
                    Object ref = title;
                    if (ref instanceof String) {
                        com.google.protobuf.ByteString b =
                                com.google.protobuf.ByteString.copyFromUtf8(
                                        (String) ref);
                        title = b;
                        return b;
                    }
                    else {
                        return (com.google.protobuf.ByteString) ref;
                    }
                }

                /**
                 * <code>string title = 1;</code>
                 */
                public Builder setTitleBytes(
                        com.google.protobuf.ByteString value)
                {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    checkByteStringIsUtf8(value);

                    title = value;
                    onChanged();
                    return this;
                }

                /**
                 * <code>string title = 1;</code>
                 */
                public Builder clearTitle()
                {
                    title = getDefaultInstance().getTitle();
                    onChanged();
                    return this;
                }

                private void ensureUrlsIsMutable()
                {
                    if (!((bitField0 & 0x00000002) == 0x00000002)) {
                        urls = new com.google.protobuf.LazyStringArrayList(urls);
                        bitField0 |= 0x00000002;
                    }
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                @Override
                public com.google.protobuf.ProtocolStringList getUrlsList()
                {
                    return urls.getUnmodifiableView();
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                @Override
                public int getUrlsCount()
                {
                    return urls.size();
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                @Override
                public String getUrls(int index)
                {
                    return urls.get(index);
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                @Override
                public com.google.protobuf.ByteString getUrlsBytes(int index)
                {
                    return urls.getByteString(index);
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                public Builder setUrls(
                        int index, String value)
                {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureUrlsIsMutable();
                    urls.set(index, value);
                    onChanged();
                    return this;
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                public Builder addUrls(
                        String value)
                {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    ensureUrlsIsMutable();
                    urls.add(value);
                    onChanged();
                    return this;
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                public Builder addAllUrls(
                        Iterable<String> values)
                {
                    ensureUrlsIsMutable();
                    com.google.protobuf.AbstractMessageLite.Builder.addAll(
                            values, urls);
                    onChanged();
                    return this;
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                public Builder clearUrls()
                {
                    urls = com.google.protobuf.LazyStringArrayList.EMPTY;
                    bitField0 = (bitField0 & ~0x00000002);
                    onChanged();
                    return this;
                }

                /**
                 * <code>repeated string urls = 2;</code>
                 */
                public Builder addUrlsBytes(
                        com.google.protobuf.ByteString value)
                {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    checkByteStringIsUtf8(value);
                    ensureUrlsIsMutable();
                    urls.add(value);
                    onChanged();
                    return this;
                }

                @Override
                public Builder setUnknownFields(
                        final com.google.protobuf.UnknownFieldSet unknownFields)
                {
                    return super.setUnknownFieldsProto3(unknownFields);
                }

                @Override
                public Builder mergeUnknownFields(
                        final com.google.protobuf.UnknownFieldSet unknownFields)
                {
                    return super.mergeUnknownFields(unknownFields);
                }

                // @@protoc_insertion_point(builder_scope:proto.SubMessage.NestedMessage)
            }
        }

        /**
         * Protobuf type {@code proto.SubMessage}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:proto.SubMessage)
                SubMessageOrBuilder
        {
            private Object foo = "";
            private double bar;
            private NestedMessage nestedMessage;
            private com.google.protobuf.SingleFieldBuilderV3<
                    NestedMessage, NestedMessage.Builder, NestedMessageOrBuilder> nestedMessageBuilder;

            // Construct using io.trino.plugin.pulsar.decoder.protobufnative.TestMsg.SubMessage.newBuilder()
            private Builder()
            {
                maybeForceBuilderInitialization();
            }

            private Builder(
                    BuilderParent parent)
            {
                super(parent);
                maybeForceBuilderInitialization();
            }

            public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
            {
                return TestMsg.internal_static_proto_SubMessage_descriptor;
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable()
            {
                return TestMsg.internal_static_proto_SubMessage_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                SubMessage.class, Builder.class);
            }

            private void maybeForceBuilderInitialization()
            {
            }

            @Override
            public Builder clear()
            {
                super.clear();
                foo = "";

                bar = 0D;

                if (nestedMessageBuilder == null) {
                    nestedMessage = null;
                }
                else {
                    nestedMessage = null;
                    nestedMessageBuilder = null;
                }
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType()
            {
                return TestMsg.internal_static_proto_SubMessage_descriptor;
            }

            @Override
            public SubMessage getDefaultInstanceForType()
            {
                return SubMessage.getDefaultInstance();
            }

            @Override
            public SubMessage build()
            {
                SubMessage result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public SubMessage buildPartial()
            {
                SubMessage result = new SubMessage(this);
                result.foo = foo;
                result.bar = bar;
                if (nestedMessageBuilder == null) {
                    result.nestedMessage = nestedMessage;
                }
                else {
                    result.nestedMessage = nestedMessageBuilder.build();
                }
                onBuilt();
                return result;
            }

            @Override
            public Builder clone()
            {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    Object value)
            {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(
                    com.google.protobuf.Descriptors.FieldDescriptor field)
            {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(
                    com.google.protobuf.Descriptors.OneofDescriptor oneof)
            {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index, Object value)
            {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    Object value)
            {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other)
            {
                if (other instanceof SubMessage) {
                    return mergeFrom((SubMessage) other);
                }
                else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(SubMessage other)
            {
                if (other == SubMessage.getDefaultInstance()) {
                    return this;
                }
                if (!other.getFoo().isEmpty()) {
                    foo = other.foo;
                    onChanged();
                }
                if (other.getBar() != 0D) {
                    setBar(other.getBar());
                }
                if (other.hasNestedMessage()) {
                    mergeNestedMessage(other.getNestedMessage());
                }
                this.mergeUnknownFields(other.unknownFields);
                onChanged();
                return this;
            }

            @Override
            public boolean isInitialized()
            {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException
            {
                SubMessage parsedMessage = null;
                try {
                    parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    parsedMessage = (SubMessage) e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        mergeFrom(parsedMessage);
                    }
                }
                return this;
            }

            /**
             * <code>string foo = 1;</code>
             */
            @Override
            public String getFoo()
            {
                Object ref = foo;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs =
                            (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    foo = s;
                    return s;
                }
                else {
                    return (String) ref;
                }
            }

            /**
             * <code>string foo = 1;</code>
             */
            public Builder setFoo(
                    String value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }

                foo = value;
                onChanged();
                return this;
            }

            /**
             * <code>string foo = 1;</code>
             */
            @Override
            public com.google.protobuf.ByteString getFooBytes()
            {
                Object ref = foo;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    foo = b;
                    return b;
                }
                else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string foo = 1;</code>
             */
            public Builder setFooBytes(
                    com.google.protobuf.ByteString value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);

                foo = value;
                onChanged();
                return this;
            }

            /**
             * <code>string foo = 1;</code>
             */
            public Builder clearFoo()
            {
                foo = getDefaultInstance().getFoo();
                onChanged();
                return this;
            }

            /**
             * <code>double bar = 2;</code>
             */
            @Override
            public double getBar()
            {
                return bar;
            }

            /**
             * <code>double bar = 2;</code>
             */
            public Builder setBar(double value)
            {
                bar = value;
                onChanged();
                return this;
            }

            /**
             * <code>double bar = 2;</code>
             */
            public Builder clearBar()
            {
                bar = 0D;
                onChanged();
                return this;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            @Override
            public boolean hasNestedMessage()
            {
                return nestedMessageBuilder != null || nestedMessage != null;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            @Override
            public NestedMessage getNestedMessage()
            {
                if (nestedMessageBuilder == null) {
                    return nestedMessage == null ? NestedMessage.getDefaultInstance() : nestedMessage;
                }
                else {
                    return nestedMessageBuilder.getMessage();
                }
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            public Builder setNestedMessage(NestedMessage value)
            {
                if (nestedMessageBuilder == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    nestedMessage = value;
                    onChanged();
                }
                else {
                    nestedMessageBuilder.setMessage(value);
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            public Builder setNestedMessage(
                    NestedMessage.Builder builderForValue)
            {
                if (nestedMessageBuilder == null) {
                    nestedMessage = builderForValue.build();
                    onChanged();
                }
                else {
                    nestedMessageBuilder.setMessage(builderForValue.build());
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            public Builder mergeNestedMessage(NestedMessage value)
            {
                if (nestedMessageBuilder == null) {
                    if (nestedMessage != null) {
                        nestedMessage =
                                NestedMessage.newBuilder(nestedMessage).mergeFrom(value).buildPartial();
                    }
                    else {
                        nestedMessage = value;
                    }
                    onChanged();
                }
                else {
                    nestedMessageBuilder.mergeFrom(value);
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            public Builder clearNestedMessage()
            {
                if (nestedMessageBuilder == null) {
                    nestedMessage = null;
                    onChanged();
                }
                else {
                    nestedMessage = null;
                    nestedMessageBuilder = null;
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            public NestedMessage.Builder getNestedMessageBuilder()
            {
                onChanged();
                return getNestedMessageFieldBuilder().getBuilder();
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            @Override
            public NestedMessageOrBuilder getNestedMessageOrBuilder()
            {
                if (nestedMessageBuilder != null) {
                    return nestedMessageBuilder.getMessageOrBuilder();
                }
                else {
                    return nestedMessage == null ?
                            NestedMessage.getDefaultInstance() : nestedMessage;
                }
            }

            /**
             * <code>.proto.SubMessage.NestedMessage nestedMessage = 3;</code>
             */
            private com.google.protobuf.SingleFieldBuilderV3<
                    NestedMessage, NestedMessage.Builder, NestedMessageOrBuilder> getNestedMessageFieldBuilder()
            {
                if (nestedMessageBuilder == null) {
                    nestedMessageBuilder = new com.google.protobuf.SingleFieldBuilderV3<
                            NestedMessage, NestedMessage.Builder, NestedMessageOrBuilder>(
                            getNestedMessage(),
                            getParentForChildren(),
                            isClean());
                    nestedMessage = null;
                }
                return nestedMessageBuilder;
            }

            @Override
            public Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields)
            {
                return super.setUnknownFieldsProto3(unknownFields);
            }

            @Override
            public Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields)
            {
                return super.mergeUnknownFields(unknownFields);
            }

            // @@protoc_insertion_point(builder_scope:proto.SubMessage)
        }
    }

    /**
     * Protobuf type {@code proto.TestMessage}
     */
    public static final class TestMessage
            extends com.google.protobuf.GeneratedMessageV3
            implements
            // @@protoc_insertion_point(message_implements:proto.TestMessage)
            TestMessageOrBuilder
    {
        public static final int STRINGFIELD_FIELD_NUMBER = 1;
        public static final int DOUBLEFIELD_FIELD_NUMBER = 2;
        public static final int FLOATFIELD_FIELD_NUMBER = 3;
        public static final int INT32FIELD_FIELD_NUMBER = 4;
        public static final int INT64FIELD_FIELD_NUMBER = 5;
        public static final int UINT32FIELD_FIELD_NUMBER = 6;
        public static final int UINT64FIELD_FIELD_NUMBER = 7;
        public static final int SINT32FIELD_FIELD_NUMBER = 8;
        public static final int SINT64FIELD_FIELD_NUMBER = 9;
        public static final int FIXED32FIELD_FIELD_NUMBER = 10;
        public static final int FIXED64FIELD_FIELD_NUMBER = 11;
        public static final int SFIXED32FIELD_FIELD_NUMBER = 12;
        public static final int SFIXED64FIELD_FIELD_NUMBER = 13;
        public static final int BOOLFIELD_FIELD_NUMBER = 14;
        public static final int BYTESFIELD_FIELD_NUMBER = 15;
        public static final int TESTENUM_FIELD_NUMBER = 16;
        public static final int SUBMESSAGE_FIELD_NUMBER = 17;
        public static final int REPEATEDFIELD_FIELD_NUMBER = 18;
        public static final int MAPFIELD_FIELD_NUMBER = 19;
        private static final long serialVersionUID = 0L;
        // @@protoc_insertion_point(class_scope:proto.TestMessage)
        private static final TestMessage DEFAULT_INSTANCE;
        private static final com.google.protobuf.Parser<TestMessage>
                PARSER = new com.google.protobuf.AbstractParser<TestMessage>()
                {
                    @Override
                    public TestMessage parsePartialFrom(com.google.protobuf.CodedInputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException
                    {
                        return new TestMessage(input, extensionRegistry);
                    }
                };

        static {
            DEFAULT_INSTANCE = new TestMessage();
        }

        private volatile Object stringField;
        private double doubleField;
        private float floatField;
        private int int32Field;
        private long int64Field;
        private int uint32Field;
        private long uint64Field;
        private int sint32Field;
        private long sint64Field;
        private int fixed32Field;
        private long fixed64Field;
        private int sfixed32Field;
        private long sfixed64Field;
        private boolean boolField;
        private com.google.protobuf.ByteString bytesField;
        private int testEnum;
        private SubMessage subMessage;
        private com.google.protobuf.LazyStringList repeatedField;
        private com.google.protobuf.MapField<
                String, Double> mapField;
        private byte memoizedIsInitialized = -1;

        // Use TestMessage.newBuilder() to construct.
        private TestMessage(com.google.protobuf.GeneratedMessageV3.Builder<?> builder)
        {
            super(builder);
        }

        private TestMessage()
        {
            stringField = "";
            doubleField = 0D;
            floatField = 0F;
            int32Field = 0;
            int64Field = 0L;
            uint32Field = 0;
            uint64Field = 0L;
            sint32Field = 0;
            sint64Field = 0L;
            fixed32Field = 0;
            fixed64Field = 0L;
            sfixed32Field = 0;
            sfixed64Field = 0L;
            boolField = false;
            bytesField = com.google.protobuf.ByteString.EMPTY;
            testEnum = 0;
            repeatedField = com.google.protobuf.LazyStringArrayList.EMPTY;
        }

        private TestMessage(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            this();
            if (extensionRegistry == null) {
                throw new NullPointerException();
            }
            int mutableBitField0 = 0;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields =
                    com.google.protobuf.UnknownFieldSet.newBuilder();
            try {
                boolean done = false;
                while (!done) {
                    int tag = input.readTag();
                    switch (tag) {
                        case 0:
                            done = true;
                            break;
                        case 10: {
                            String s = input.readStringRequireUtf8();

                            stringField = s;
                            break;
                        }
                        case 17: {
                            doubleField = input.readDouble();
                            break;
                        }
                        case 29: {
                            floatField = input.readFloat();
                            break;
                        }
                        case 32: {
                            int32Field = input.readInt32();
                            break;
                        }
                        case 40: {
                            int64Field = input.readInt64();
                            break;
                        }
                        case 48: {
                            uint32Field = input.readUInt32();
                            break;
                        }
                        case 56: {
                            uint64Field = input.readUInt64();
                            break;
                        }
                        case 64: {
                            sint32Field = input.readSInt32();
                            break;
                        }
                        case 72: {
                            sint64Field = input.readSInt64();
                            break;
                        }
                        case 85: {
                            fixed32Field = input.readFixed32();
                            break;
                        }
                        case 89: {
                            fixed64Field = input.readFixed64();
                            break;
                        }
                        case 101: {
                            sfixed32Field = input.readSFixed32();
                            break;
                        }
                        case 105: {
                            sfixed64Field = input.readSFixed64();
                            break;
                        }
                        case 112: {
                            boolField = input.readBool();
                            break;
                        }
                        case 122: {
                            bytesField = input.readBytes();
                            break;
                        }
                        case 128: {
                            int rawValue = input.readEnum();
                            testEnum = rawValue;
                            break;
                        }
                        case 138: {
                            SubMessage.Builder subBuilder = null;
                            if (subMessage != null) {
                                subBuilder = subMessage.toBuilder();
                            }
                            subMessage = input.readMessage(SubMessage.parser(), extensionRegistry);
                            if (subBuilder != null) {
                                subBuilder.mergeFrom(subMessage);
                                subMessage = subBuilder.buildPartial();
                            }

                            break;
                        }
                        case 146: {
                            String s = input.readStringRequireUtf8();
                            if (!((mutableBitField0 & 0x00020000) == 0x00020000)) {
                                repeatedField = new com.google.protobuf.LazyStringArrayList();
                                mutableBitField0 |= 0x00020000;
                            }
                            repeatedField.add(s);
                            break;
                        }
                        case 154: {
                            if (!((mutableBitField0 & 0x00040000) == 0x00040000)) {
                                mapField = com.google.protobuf.MapField.newMapField(
                                        MapFieldDefaultEntryHolder.defaultEntry);
                                mutableBitField0 |= 0x00040000;
                            }
                            com.google.protobuf.MapEntry<String, Double>
                                    inputMapField = input.readMessage(
                                    MapFieldDefaultEntryHolder.defaultEntry.getParserForType(), extensionRegistry);
                            mapField.getMutableMap().put(
                                    inputMapField.getKey(), inputMapField.getValue());
                            break;
                        }
                        default: {
                            if (!parseUnknownFieldProto3(
                                    input, unknownFields, extensionRegistry, tag)) {
                                done = true;
                            }
                            break;
                        }
                    }
                }
            }
            catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage(this);
            }
            catch (java.io.IOException e) {
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        e).setUnfinishedMessage(this);
            }
            finally {
                if (((mutableBitField0 & 0x00020000) == 0x00020000)) {
                    repeatedField = repeatedField.getUnmodifiableView();
                }
                this.unknownFields = unknownFields.build();
                makeExtensionsImmutable();
            }
        }

        public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
        {
            return TestMsg.internal_static_proto_TestMessage_descriptor;
        }

        public static TestMessage parseFrom(
                java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static TestMessage parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static TestMessage parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static TestMessage parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static TestMessage parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data);
        }

        public static TestMessage parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException
        {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static TestMessage parseFrom(java.io.InputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input);
        }

        public static TestMessage parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input, extensionRegistry);
        }

        public static TestMessage parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseDelimitedWithIOException(PARSER, input);
        }

        public static TestMessage parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
        }

        public static TestMessage parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input);
        }

        public static TestMessage parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException
        {
            return com.google.protobuf.GeneratedMessageV3
                    .parseWithIOException(PARSER, input, extensionRegistry);
        }

        public static Builder newBuilder()
        {
            return DEFAULT_INSTANCE.toBuilder();
        }

        public static Builder newBuilder(TestMessage prototype)
        {
            return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
        }

        public static TestMessage getDefaultInstance()
        {
            return DEFAULT_INSTANCE;
        }

        public static com.google.protobuf.Parser<TestMessage> parser()
        {
            return PARSER;
        }

        @Override
        public com.google.protobuf.UnknownFieldSet getUnknownFields()
        {
            return this.unknownFields;
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected com.google.protobuf.MapField internalGetMapField(
                int number)
        {
            switch (number) {
                case 19:
                    return internalGetMapField();
                default:
                    throw new RuntimeException(
                            "Invalid map field number: " + number);
            }
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable()
        {
            return TestMsg.internal_static_proto_TestMessage_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            TestMessage.class, Builder.class);
        }

        /**
         * <code>string stringField = 1;</code>
         */
        @Override
        public String getStringField()
        {
            Object ref = stringField;
            if (ref instanceof String) {
                return (String) ref;
            }
            else {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                stringField = s;
                return s;
            }
        }

        /**
         * <code>string stringField = 1;</code>
         */
        @Override
        public com.google.protobuf.ByteString getStringFieldBytes()
        {
            Object ref = stringField;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (String) ref);
                stringField = b;
                return b;
            }
            else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <code>double doubleField = 2;</code>
         */
        @Override
        public double getDoubleField()
        {
            return doubleField;
        }

        /**
         * <code>float floatField = 3;</code>
         */
        @Override
        public float getFloatField()
        {
            return floatField;
        }

        /**
         * <code>int32 int32Field = 4;</code>
         */
        @Override
        public int getInt32Field()
        {
            return int32Field;
        }

        /**
         * <code>int64 int64Field = 5;</code>
         */
        @Override
        public long getInt64Field()
        {
            return int64Field;
        }

        /**
         * <code>uint32 uint32Field = 6;</code>
         */
        @Override
        public int getUint32Field()
        {
            return uint32Field;
        }

        /**
         * <code>uint64 uint64Field = 7;</code>
         */
        @Override
        public long getUint64Field()
        {
            return uint64Field;
        }

        /**
         * <code>sint32 sint32Field = 8;</code>
         */
        @Override
        public int getSint32Field()
        {
            return sint32Field;
        }

        /**
         * <code>sint64 sint64Field = 9;</code>
         */
        @Override
        public long getSint64Field()
        {
            return sint64Field;
        }

        /**
         * <code>fixed32 fixed32Field = 10;</code>
         */
        @Override
        public int getFixed32Field()
        {
            return fixed32Field;
        }

        /**
         * <code>fixed64 fixed64Field = 11;</code>
         */
        @Override
        public long getFixed64Field()
        {
            return fixed64Field;
        }

        /**
         * <code>sfixed32 sfixed32Field = 12;</code>
         */
        @Override
        public int getSfixed32Field()
        {
            return sfixed32Field;
        }

        /**
         * <code>sfixed64 sfixed64Field = 13;</code>
         */
        @Override
        public long getSfixed64Field()
        {
            return sfixed64Field;
        }

        /**
         * <code>bool boolField = 14;</code>
         */
        @Override
        public boolean getBoolField()
        {
            return boolField;
        }

        /**
         * <code>bytes bytesField = 15;</code>
         */
        @Override
        public com.google.protobuf.ByteString getBytesField()
        {
            return bytesField;
        }

        /**
         * <code>.proto.TestEnum testEnum = 16;</code>
         */
        @Override
        public int getTestEnumValue()
        {
            return testEnum;
        }

        /**
         * <code>.proto.TestEnum testEnum = 16;</code>
         */
        @Override
        public TestEnum getTestEnum()
        {
            TestEnum result = TestEnum.valueOf(testEnum);
            return result == null ? TestEnum.UNRECOGNIZED : result;
        }

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        @Override
        public boolean hasSubMessage()
        {
            return subMessage != null;
        }

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        @Override
        public SubMessage getSubMessage()
        {
            return subMessage == null ? SubMessage.getDefaultInstance() : subMessage;
        }

        /**
         * <code>.proto.SubMessage subMessage = 17;</code>
         */
        @Override
        public SubMessageOrBuilder getSubMessageOrBuilder()
        {
            return getSubMessage();
        }

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        @Override
        public com.google.protobuf.ProtocolStringList getRepeatedFieldList()
        {
            return repeatedField;
        }

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        @Override
        public int getRepeatedFieldCount()
        {
            return repeatedField.size();
        }

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        @Override
        public String getRepeatedField(int index)
        {
            return repeatedField.get(index);
        }

        /**
         * <code>repeated string repeatedField = 18;</code>
         */
        @Override
        public com.google.protobuf.ByteString getRepeatedFieldBytes(int index)
        {
            return repeatedField.getByteString(index);
        }

        private com.google.protobuf.MapField<String, Double> internalGetMapField()
        {
            if (mapField == null) {
                return com.google.protobuf.MapField.emptyMapField(
                        MapFieldDefaultEntryHolder.defaultEntry);
            }
            return mapField;
        }

        @Override
        public int getMapFieldCount()
        {
            return internalGetMapField().getMap().size();
        }

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        @Override
        public boolean containsMapField(
                String key)
        {
            if (key == null) {
                throw new NullPointerException();
            }
            return internalGetMapField().getMap().containsKey(key);
        }

        /**
         * Use {@link #getMapFieldMap()} instead.
         */
        @Override
        @Deprecated
        public java.util.Map<String, Double> getMapField()
        {
            return getMapFieldMap();
        }

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        @Override
        public java.util.Map<String, Double> getMapFieldMap()
        {
            return internalGetMapField().getMap();
        }

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */

        @Override
        public double getMapFieldOrDefault(
                String key,
                double defaultValue)
        {
            if (key == null) {
                throw new NullPointerException();
            }
            java.util.Map<String, Double> map =
                    internalGetMapField().getMap();
            return map.containsKey(key) ? map.get(key) : defaultValue;
        }

        /**
         * <code>map&lt;string, double&gt; mapField = 19;</code>
         */
        @Override
        public double getMapFieldOrThrow(
                String key)
        {
            if (key == null) {
                throw new NullPointerException();
            }
            java.util.Map<String, Double> map =
                    internalGetMapField().getMap();
            if (!map.containsKey(key)) {
                throw new IllegalArgumentException();
            }
            return map.get(key);
        }

        @Override
        public boolean isInitialized()
        {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized == 1) {
                return true;
            }
            if (isInitialized == 0) {
                return false;
            }

            memoizedIsInitialized = 1;
            return true;
        }

        @Override
        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException
        {
            if (!getStringFieldBytes().isEmpty()) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 1, stringField);
            }
            if (doubleField != 0D) {
                output.writeDouble(2, doubleField);
            }
            if (floatField != 0F) {
                output.writeFloat(3, floatField);
            }
            if (int32Field != 0) {
                output.writeInt32(4, int32Field);
            }
            if (int64Field != 0L) {
                output.writeInt64(5, int64Field);
            }
            if (uint32Field != 0) {
                output.writeUInt32(6, uint32Field);
            }
            if (uint64Field != 0L) {
                output.writeUInt64(7, uint64Field);
            }
            if (sint32Field != 0) {
                output.writeSInt32(8, sint32Field);
            }
            if (sint64Field != 0L) {
                output.writeSInt64(9, sint64Field);
            }
            if (fixed32Field != 0) {
                output.writeFixed32(10, fixed32Field);
            }
            if (fixed64Field != 0L) {
                output.writeFixed64(11, fixed64Field);
            }
            if (sfixed32Field != 0) {
                output.writeSFixed32(12, sfixed32Field);
            }
            if (sfixed64Field != 0L) {
                output.writeSFixed64(13, sfixed64Field);
            }
            if (boolField) {
                output.writeBool(14, boolField);
            }
            if (!bytesField.isEmpty()) {
                output.writeBytes(15, bytesField);
            }
            if (testEnum != TestEnum.SHARED.getNumber()) {
                output.writeEnum(16, testEnum);
            }
            if (subMessage != null) {
                output.writeMessage(17, getSubMessage());
            }
            for (int i = 0; i < repeatedField.size(); i++) {
                com.google.protobuf.GeneratedMessageV3.writeString(output, 18, repeatedField.getRaw(i));
            }
            com.google.protobuf.GeneratedMessageV3
                    .serializeStringMapTo(
                            output,
                            internalGetMapField(),
                            MapFieldDefaultEntryHolder.defaultEntry,
                            19);
            unknownFields.writeTo(output);
        }

        @Override
        public int getSerializedSize()
        {
            int size = memoizedSize;
            if (size != -1) {
                return size;
            }

            size = 0;
            if (!getStringFieldBytes().isEmpty()) {
                size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, stringField);
            }
            if (doubleField != 0D) {
                size += com.google.protobuf.CodedOutputStream
                        .computeDoubleSize(2, doubleField);
            }
            if (floatField != 0F) {
                size += com.google.protobuf.CodedOutputStream
                        .computeFloatSize(3, floatField);
            }
            if (int32Field != 0) {
                size += com.google.protobuf.CodedOutputStream
                        .computeInt32Size(4, int32Field);
            }
            if (int64Field != 0L) {
                size += com.google.protobuf.CodedOutputStream
                        .computeInt64Size(5, int64Field);
            }
            if (uint32Field != 0) {
                size += com.google.protobuf.CodedOutputStream
                        .computeUInt32Size(6, uint32Field);
            }
            if (uint64Field != 0L) {
                size += com.google.protobuf.CodedOutputStream
                        .computeUInt64Size(7, uint64Field);
            }
            if (sint32Field != 0) {
                size += com.google.protobuf.CodedOutputStream
                        .computeSInt32Size(8, sint32Field);
            }
            if (sint64Field != 0L) {
                size += com.google.protobuf.CodedOutputStream
                        .computeSInt64Size(9, sint64Field);
            }
            if (fixed32Field != 0) {
                size += com.google.protobuf.CodedOutputStream
                        .computeFixed32Size(10, fixed32Field);
            }
            if (fixed64Field != 0L) {
                size += com.google.protobuf.CodedOutputStream
                        .computeFixed64Size(11, fixed64Field);
            }
            if (sfixed32Field != 0) {
                size += com.google.protobuf.CodedOutputStream
                        .computeSFixed32Size(12, sfixed32Field);
            }
            if (sfixed64Field != 0L) {
                size += com.google.protobuf.CodedOutputStream
                        .computeSFixed64Size(13, sfixed64Field);
            }
            if (boolField) {
                size += com.google.protobuf.CodedOutputStream
                        .computeBoolSize(14, boolField);
            }
            if (!bytesField.isEmpty()) {
                size += com.google.protobuf.CodedOutputStream
                        .computeBytesSize(15, bytesField);
            }
            if (testEnum != TestEnum.SHARED.getNumber()) {
                size += com.google.protobuf.CodedOutputStream
                        .computeEnumSize(16, testEnum);
            }
            if (subMessage != null) {
                size += com.google.protobuf.CodedOutputStream
                        .computeMessageSize(17, getSubMessage());
            }
            {
                int dataSize = 0;
                for (int i = 0; i < repeatedField.size(); i++) {
                    dataSize += computeStringSizeNoTag(repeatedField.getRaw(i));
                }
                size += dataSize;
                size += 2 * getRepeatedFieldList().size();
            }
            for (java.util.Map.Entry<String, Double> entry
                    : internalGetMapField().getMap().entrySet()) {
                com.google.protobuf.MapEntry<String, Double>
                        mapField = MapFieldDefaultEntryHolder.defaultEntry.newBuilderForType()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                        .build();
                size += com.google.protobuf.CodedOutputStream
                        .computeMessageSize(19, mapField);
            }
            size += unknownFields.getSerializedSize();
            memoizedSize = size;
            return size;
        }

        @Override
        public boolean equals(final Object obj)
        {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof TestMessage other)) {
                return super.equals(obj);
            }

            boolean result = true;
            result = result && getStringField()
                    .equals(other.getStringField());
            result = result && (
                    Double.doubleToLongBits(getDoubleField())
                            == Double.doubleToLongBits(
                            other.getDoubleField()));
            result = result && (
                    Float.floatToIntBits(getFloatField())
                            == Float.floatToIntBits(
                            other.getFloatField()));
            result = result && (getInt32Field()
                    == other.getInt32Field());
            result = result && (getInt64Field()
                    == other.getInt64Field());
            result = result && (getUint32Field()
                    == other.getUint32Field());
            result = result && (getUint64Field()
                    == other.getUint64Field());
            result = result && (getSint32Field()
                    == other.getSint32Field());
            result = result && (getSint64Field()
                    == other.getSint64Field());
            result = result && (getFixed32Field()
                    == other.getFixed32Field());
            result = result && (getFixed64Field()
                    == other.getFixed64Field());
            result = result && (getSfixed32Field()
                    == other.getSfixed32Field());
            result = result && (getSfixed64Field()
                    == other.getSfixed64Field());
            result = result && (getBoolField()
                    == other.getBoolField());
            result = result && getBytesField()
                    .equals(other.getBytesField());
            result = result && testEnum == other.testEnum;
            result = result && (hasSubMessage() == other.hasSubMessage());
            if (hasSubMessage()) {
                result = result && getSubMessage()
                        .equals(other.getSubMessage());
            }
            result = result && getRepeatedFieldList()
                    .equals(other.getRepeatedFieldList());
            result = result && internalGetMapField().equals(
                    other.internalGetMapField());
            result = result && unknownFields.equals(other.unknownFields);
            return result;
        }

        @Override
        public int hashCode()
        {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }
            int hash = 41;
            hash = (19 * hash) + getDescriptor().hashCode();
            hash = (37 * hash) + STRINGFIELD_FIELD_NUMBER;
            hash = (53 * hash) + getStringField().hashCode();
            hash = (37 * hash) + DOUBLEFIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    Double.doubleToLongBits(getDoubleField()));
            hash = (37 * hash) + FLOATFIELD_FIELD_NUMBER;
            hash = (53 * hash) + Float.floatToIntBits(
                    getFloatField());
            hash = (37 * hash) + INT32FIELD_FIELD_NUMBER;
            hash = (53 * hash) + getInt32Field();
            hash = (37 * hash) + INT64FIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    getInt64Field());
            hash = (37 * hash) + UINT32FIELD_FIELD_NUMBER;
            hash = (53 * hash) + getUint32Field();
            hash = (37 * hash) + UINT64FIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    getUint64Field());
            hash = (37 * hash) + SINT32FIELD_FIELD_NUMBER;
            hash = (53 * hash) + getSint32Field();
            hash = (37 * hash) + SINT64FIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    getSint64Field());
            hash = (37 * hash) + FIXED32FIELD_FIELD_NUMBER;
            hash = (53 * hash) + getFixed32Field();
            hash = (37 * hash) + FIXED64FIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    getFixed64Field());
            hash = (37 * hash) + SFIXED32FIELD_FIELD_NUMBER;
            hash = (53 * hash) + getSfixed32Field();
            hash = (37 * hash) + SFIXED64FIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
                    getSfixed64Field());
            hash = (37 * hash) + BOOLFIELD_FIELD_NUMBER;
            hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
                    getBoolField());
            hash = (37 * hash) + BYTESFIELD_FIELD_NUMBER;
            hash = (53 * hash) + getBytesField().hashCode();
            hash = (37 * hash) + TESTENUM_FIELD_NUMBER;
            hash = (53 * hash) + testEnum;
            if (hasSubMessage()) {
                hash = (37 * hash) + SUBMESSAGE_FIELD_NUMBER;
                hash = (53 * hash) + getSubMessage().hashCode();
            }
            if (getRepeatedFieldCount() > 0) {
                hash = (37 * hash) + REPEATEDFIELD_FIELD_NUMBER;
                hash = (53 * hash) + getRepeatedFieldList().hashCode();
            }
            if (!internalGetMapField().getMap().isEmpty()) {
                hash = (37 * hash) + MAPFIELD_FIELD_NUMBER;
                hash = (53 * hash) + internalGetMapField().hashCode();
            }
            hash = (29 * hash) + unknownFields.hashCode();
            memoizedHashCode = hash;
            return hash;
        }

        @Override
        public Builder newBuilderForType()
        {
            return newBuilder();
        }

        @Override
        public Builder toBuilder()
        {
            return this == DEFAULT_INSTANCE
                    ? new Builder() : new Builder().mergeFrom(this);
        }

        @Override
        protected Builder newBuilderForType(
                BuilderParent parent)
        {
            Builder builder = new Builder(parent);
            return builder;
        }

        @Override
        public com.google.protobuf.Parser<TestMessage> getParserForType()
        {
            return PARSER;
        }

        @Override
        public TestMessage getDefaultInstanceForType()
        {
            return DEFAULT_INSTANCE;
        }

        private static final class MapFieldDefaultEntryHolder
        {
            static final com.google.protobuf.MapEntry<
                    String, Double> defaultEntry =
                    com.google.protobuf.MapEntry
                            .newDefaultInstance(
                                    TestMsg.internal_static_proto_TestMessage_MapFieldEntry_descriptor,
                                    com.google.protobuf.WireFormat.FieldType.STRING,
                                    "",
                                    com.google.protobuf.WireFormat.FieldType.DOUBLE,
                                    0D);
        }

        /**
         * Protobuf type {@code proto.TestMessage}
         */
        public static final class Builder
                extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
                implements
                // @@protoc_insertion_point(builder_implements:proto.TestMessage)
                TestMessageOrBuilder
        {
            private int bitField0;
            private Object stringField = "";
            private double doubleField;
            private float floatField;
            private int int32Field;
            private long int64Field;
            private int uint32Field;
            private long uint64Field;
            private int sint32Field;
            private long sint64Field;
            private int fixed32Field;
            private long fixed64Field;
            private int sfixed32Field;
            private long sfixed64Field;
            private boolean boolField;
            private com.google.protobuf.ByteString bytesField = com.google.protobuf.ByteString.EMPTY;
            private int testEnum;
            private SubMessage subMessage;
            private com.google.protobuf.SingleFieldBuilderV3<
                    SubMessage, SubMessage.Builder, SubMessageOrBuilder> subMessageBuilder;
            private com.google.protobuf.LazyStringList repeatedField = com.google.protobuf.LazyStringArrayList.EMPTY;
            private com.google.protobuf.MapField<
                    String, Double> mapField;

            // Construct using io.trino.plugin.pulsar.decoder.protobufnative.TestMsg.TestMessage.newBuilder()
            private Builder()
            {
                maybeForceBuilderInitialization();
            }

            private Builder(
                    BuilderParent parent)
            {
                super(parent);
                maybeForceBuilderInitialization();
            }

            public static com.google.protobuf.Descriptors.Descriptor getDescriptor()
            {
                return TestMsg.internal_static_proto_TestMessage_descriptor;
            }

            @SuppressWarnings("rawtypes")
            @Override
            protected com.google.protobuf.MapField internalGetMapField(
                    int number)
            {
                switch (number) {
                    case 19:
                        return internalGetMapField();
                    default:
                        throw new RuntimeException(
                                "Invalid map field number: " + number);
                }
            }

            @SuppressWarnings("rawtypes")
            @Override
            protected com.google.protobuf.MapField internalGetMutableMapField(
                    int number)
            {
                switch (number) {
                    case 19:
                        return internalGetMutableMapField();
                    default:
                        throw new RuntimeException(
                                "Invalid map field number: " + number);
                }
            }

            @Override
            protected FieldAccessorTable internalGetFieldAccessorTable()
            {
                return TestMsg.internal_static_proto_TestMessage_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                TestMessage.class, Builder.class);
            }

            private void maybeForceBuilderInitialization()
            {
            }

            @Override
            public Builder clear()
            {
                super.clear();
                stringField = "";

                doubleField = 0D;

                floatField = 0F;

                int32Field = 0;

                int64Field = 0L;

                uint32Field = 0;

                uint64Field = 0L;

                sint32Field = 0;

                sint64Field = 0L;

                fixed32Field = 0;

                fixed64Field = 0L;

                sfixed32Field = 0;

                sfixed64Field = 0L;

                boolField = false;

                bytesField = com.google.protobuf.ByteString.EMPTY;

                testEnum = 0;

                if (subMessageBuilder == null) {
                    subMessage = null;
                }
                else {
                    subMessage = null;
                    subMessageBuilder = null;
                }
                repeatedField = com.google.protobuf.LazyStringArrayList.EMPTY;
                bitField0 = (bitField0 & ~0x00020000);
                internalGetMutableMapField().clear();
                return this;
            }

            @Override
            public com.google.protobuf.Descriptors.Descriptor getDescriptorForType()
            {
                return TestMsg.internal_static_proto_TestMessage_descriptor;
            }

            @Override
            public TestMessage getDefaultInstanceForType()
            {
                return TestMessage.getDefaultInstance();
            }

            @Override
            public TestMessage build()
            {
                TestMessage result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            @Override
            public TestMessage buildPartial()
            {
                TestMessage result = new TestMessage(this);
                result.stringField = stringField;
                result.doubleField = doubleField;
                result.floatField = floatField;
                result.int32Field = int32Field;
                result.int64Field = int64Field;
                result.uint32Field = uint32Field;
                result.uint64Field = uint64Field;
                result.sint32Field = sint32Field;
                result.sint64Field = sint64Field;
                result.fixed32Field = fixed32Field;
                result.fixed64Field = fixed64Field;
                result.sfixed32Field = sfixed32Field;
                result.sfixed64Field = sfixed64Field;
                result.boolField = boolField;
                result.bytesField = bytesField;
                result.testEnum = testEnum;
                if (subMessageBuilder == null) {
                    result.subMessage = subMessage;
                }
                else {
                    result.subMessage = subMessageBuilder.build();
                }
                if (((bitField0 & 0x00020000) == 0x00020000)) {
                    repeatedField = repeatedField.getUnmodifiableView();
                    bitField0 = (bitField0 & ~0x00020000);
                }
                result.repeatedField = repeatedField;
                result.mapField = internalGetMapField();
                result.mapField.makeImmutable();
                onBuilt();
                return result;
            }

            @Override
            public Builder clone()
            {
                return super.clone();
            }

            @Override
            public Builder setField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    Object value)
            {
                return super.setField(field, value);
            }

            @Override
            public Builder clearField(
                    com.google.protobuf.Descriptors.FieldDescriptor field)
            {
                return super.clearField(field);
            }

            @Override
            public Builder clearOneof(
                    com.google.protobuf.Descriptors.OneofDescriptor oneof)
            {
                return super.clearOneof(oneof);
            }

            @Override
            public Builder setRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    int index, Object value)
            {
                return super.setRepeatedField(field, index, value);
            }

            @Override
            public Builder addRepeatedField(
                    com.google.protobuf.Descriptors.FieldDescriptor field,
                    Object value)
            {
                return super.addRepeatedField(field, value);
            }

            @Override
            public Builder mergeFrom(com.google.protobuf.Message other)
            {
                if (other instanceof TestMessage) {
                    return mergeFrom((TestMessage) other);
                }
                else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(TestMessage other)
            {
                if (other == TestMessage.getDefaultInstance()) {
                    return this;
                }
                if (!other.getStringField().isEmpty()) {
                    stringField = other.stringField;
                    onChanged();
                }
                if (other.getDoubleField() != 0D) {
                    setDoubleField(other.getDoubleField());
                }
                if (other.getFloatField() != 0F) {
                    setFloatField(other.getFloatField());
                }
                if (other.getInt32Field() != 0) {
                    setInt32Field(other.getInt32Field());
                }
                if (other.getInt64Field() != 0L) {
                    setInt64Field(other.getInt64Field());
                }
                if (other.getUint32Field() != 0) {
                    setUint32Field(other.getUint32Field());
                }
                if (other.getUint64Field() != 0L) {
                    setUint64Field(other.getUint64Field());
                }
                if (other.getSint32Field() != 0) {
                    setSint32Field(other.getSint32Field());
                }
                if (other.getSint64Field() != 0L) {
                    setSint64Field(other.getSint64Field());
                }
                if (other.getFixed32Field() != 0) {
                    setFixed32Field(other.getFixed32Field());
                }
                if (other.getFixed64Field() != 0L) {
                    setFixed64Field(other.getFixed64Field());
                }
                if (other.getSfixed32Field() != 0) {
                    setSfixed32Field(other.getSfixed32Field());
                }
                if (other.getSfixed64Field() != 0L) {
                    setSfixed64Field(other.getSfixed64Field());
                }
                if (other.getBoolField()) {
                    setBoolField(other.getBoolField());
                }
                if (other.getBytesField() != com.google.protobuf.ByteString.EMPTY) {
                    setBytesField(other.getBytesField());
                }
                if (other.testEnum != 0) {
                    setTestEnumValue(other.getTestEnumValue());
                }
                if (other.hasSubMessage()) {
                    mergeSubMessage(other.getSubMessage());
                }
                if (!other.repeatedField.isEmpty()) {
                    if (repeatedField.isEmpty()) {
                        repeatedField = other.repeatedField;
                        bitField0 = (bitField0 & ~0x00020000);
                    }
                    else {
                        ensureRepeatedFieldIsMutable();
                        repeatedField.addAll(other.repeatedField);
                    }
                    onChanged();
                }
                internalGetMutableMapField().mergeFrom(
                        other.internalGetMapField());
                this.mergeUnknownFields(other.unknownFields);
                onChanged();
                return this;
            }

            @Override
            public boolean isInitialized()
            {
                return true;
            }

            @Override
            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException
            {
                TestMessage parsedMessage = null;
                try {
                    parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
                }
                catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    parsedMessage = (TestMessage) e.getUnfinishedMessage();
                    throw e.unwrapIOException();
                }
                finally {
                    if (parsedMessage != null) {
                        mergeFrom(parsedMessage);
                    }
                }
                return this;
            }

            /**
             * <code>string stringField = 1;</code>
             */
            @Override
            public String getStringField()
            {
                Object ref = stringField;
                if (!(ref instanceof String)) {
                    com.google.protobuf.ByteString bs =
                            (com.google.protobuf.ByteString) ref;
                    String s = bs.toStringUtf8();
                    stringField = s;
                    return s;
                }
                else {
                    return (String) ref;
                }
            }

            /**
             * <code>string stringField = 1;</code>
             */
            public Builder setStringField(
                    String value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }

                stringField = value;
                onChanged();
                return this;
            }

            /**
             * <code>string stringField = 1;</code>
             */
            @Override
            public com.google.protobuf.ByteString getStringFieldBytes()
            {
                Object ref = stringField;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    stringField = b;
                    return b;
                }
                else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>string stringField = 1;</code>
             */
            public Builder setStringFieldBytes(
                    com.google.protobuf.ByteString value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);

                stringField = value;
                onChanged();
                return this;
            }

            /**
             * <code>string stringField = 1;</code>
             */
            public Builder clearStringField()
            {
                stringField = getDefaultInstance().getStringField();
                onChanged();
                return this;
            }

            /**
             * <code>double doubleField = 2;</code>
             */
            @Override
            public double getDoubleField()
            {
                return doubleField;
            }

            /**
             * <code>double doubleField = 2;</code>
             */
            public Builder setDoubleField(double value)
            {
                doubleField = value;
                onChanged();
                return this;
            }

            /**
             * <code>double doubleField = 2;</code>
             */
            public Builder clearDoubleField()
            {
                doubleField = 0D;
                onChanged();
                return this;
            }

            /**
             * <code>float floatField = 3;</code>
             */
            @Override
            public float getFloatField()
            {
                return floatField;
            }

            /**
             * <code>float floatField = 3;</code>
             */
            public Builder setFloatField(float value)
            {
                floatField = value;
                onChanged();
                return this;
            }

            /**
             * <code>float floatField = 3;</code>
             */
            public Builder clearFloatField()
            {
                floatField = 0F;
                onChanged();
                return this;
            }

            /**
             * <code>int32 int32Field = 4;</code>
             */
            @Override
            public int getInt32Field()
            {
                return int32Field;
            }

            /**
             * <code>int32 int32Field = 4;</code>
             */
            public Builder setInt32Field(int value)
            {
                int32Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>int32 int32Field = 4;</code>
             */
            public Builder clearInt32Field()
            {
                int32Field = 0;
                onChanged();
                return this;
            }

            /**
             * <code>int64 int64Field = 5;</code>
             */
            @Override
            public long getInt64Field()
            {
                return int64Field;
            }

            /**
             * <code>int64 int64Field = 5;</code>
             */
            public Builder setInt64Field(long value)
            {
                int64Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>int64 int64Field = 5;</code>
             */
            public Builder clearInt64Field()
            {
                int64Field = 0L;
                onChanged();
                return this;
            }

            /**
             * <code>uint32 uint32Field = 6;</code>
             */
            @Override
            public int getUint32Field()
            {
                return uint32Field;
            }

            /**
             * <code>uint32 uint32Field = 6;</code>
             */
            public Builder setUint32Field(int value)
            {
                uint32Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>uint32 uint32Field = 6;</code>
             */
            public Builder clearUint32Field()
            {
                uint32Field = 0;
                onChanged();
                return this;
            }

            /**
             * <code>uint64 uint64Field = 7;</code>
             */
            @Override
            public long getUint64Field()
            {
                return uint64Field;
            }

            /**
             * <code>uint64 uint64Field = 7;</code>
             */
            public Builder setUint64Field(long value)
            {
                uint64Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>uint64 uint64Field = 7;</code>
             */
            public Builder clearUint64Field()
            {
                uint64Field = 0L;
                onChanged();
                return this;
            }

            /**
             * <code>sint32 sint32Field = 8;</code>
             */
            @Override
            public int getSint32Field()
            {
                return sint32Field;
            }

            /**
             * <code>sint32 sint32Field = 8;</code>
             */
            public Builder setSint32Field(int value)
            {
                sint32Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>sint32 sint32Field = 8;</code>
             */
            public Builder clearSint32Field()
            {
                sint32Field = 0;
                onChanged();
                return this;
            }

            /**
             * <code>sint64 sint64Field = 9;</code>
             */
            @Override
            public long getSint64Field()
            {
                return sint64Field;
            }

            /**
             * <code>sint64 sint64Field = 9;</code>
             */
            public Builder setSint64Field(long value)
            {
                sint64Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>sint64 sint64Field = 9;</code>
             */
            public Builder clearSint64Field()
            {
                sint64Field = 0L;
                onChanged();
                return this;
            }

            /**
             * <code>fixed32 fixed32Field = 10;</code>
             */
            @Override
            public int getFixed32Field()
            {
                return fixed32Field;
            }

            /**
             * <code>fixed32 fixed32Field = 10;</code>
             */
            public Builder setFixed32Field(int value)
            {
                fixed32Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>fixed32 fixed32Field = 10;</code>
             */
            public Builder clearFixed32Field()
            {
                fixed32Field = 0;
                onChanged();
                return this;
            }

            /**
             * <code>fixed64 fixed64Field = 11;</code>
             */
            @Override
            public long getFixed64Field()
            {
                return fixed64Field;
            }

            /**
             * <code>fixed64 fixed64Field = 11;</code>
             */
            public Builder setFixed64Field(long value)
            {
                fixed64Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>fixed64 fixed64Field = 11;</code>
             */
            public Builder clearFixed64Field()
            {
                fixed64Field = 0L;
                onChanged();
                return this;
            }

            /**
             * <code>sfixed32 sfixed32Field = 12;</code>
             */
            @Override
            public int getSfixed32Field()
            {
                return sfixed32Field;
            }

            /**
             * <code>sfixed32 sfixed32Field = 12;</code>
             */
            public Builder setSfixed32Field(int value)
            {
                sfixed32Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>sfixed32 sfixed32Field = 12;</code>
             */
            public Builder clearSfixed32Field()
            {
                sfixed32Field = 0;
                onChanged();
                return this;
            }

            /**
             * <code>sfixed64 sfixed64Field = 13;</code>
             */
            @Override
            public long getSfixed64Field()
            {
                return sfixed64Field;
            }

            /**
             * <code>sfixed64 sfixed64Field = 13;</code>
             */
            public Builder setSfixed64Field(long value)
            {
                sfixed64Field = value;
                onChanged();
                return this;
            }

            /**
             * <code>sfixed64 sfixed64Field = 13;</code>
             */
            public Builder clearSfixed64Field()
            {
                sfixed64Field = 0L;
                onChanged();
                return this;
            }

            /**
             * <code>bool boolField = 14;</code>
             */
            @Override
            public boolean getBoolField()
            {
                return boolField;
            }

            /**
             * <code>bool boolField = 14;</code>
             */
            public Builder setBoolField(boolean value)
            {
                boolField = value;
                onChanged();
                return this;
            }

            /**
             * <code>bool boolField = 14;</code>
             */
            public Builder clearBoolField()
            {
                boolField = false;
                onChanged();
                return this;
            }

            /**
             * <code>bytes bytesField = 15;</code>
             */
            @Override
            public com.google.protobuf.ByteString getBytesField()
            {
                return bytesField;
            }

            /**
             * <code>bytes bytesField = 15;</code>
             */
            public Builder setBytesField(com.google.protobuf.ByteString value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }

                bytesField = value;
                onChanged();
                return this;
            }

            /**
             * <code>bytes bytesField = 15;</code>
             */
            public Builder clearBytesField()
            {
                bytesField = getDefaultInstance().getBytesField();
                onChanged();
                return this;
            }

            /**
             * <code>.proto.TestEnum testEnum = 16;</code>
             */
            @Override
            public int getTestEnumValue()
            {
                return testEnum;
            }

            /**
             * <code>.proto.TestEnum testEnum = 16;</code>
             */
            public Builder setTestEnumValue(int value)
            {
                testEnum = value;
                onChanged();
                return this;
            }

            /**
             * <code>.proto.TestEnum testEnum = 16;</code>
             */
            @Override
            public TestEnum getTestEnum()
            {
                TestEnum result = TestEnum.valueOf(testEnum);
                return result == null ? TestEnum.UNRECOGNIZED : result;
            }

            /**
             * <code>.proto.TestEnum testEnum = 16;</code>
             */
            public Builder setTestEnum(TestEnum value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }

                testEnum = value.getNumber();
                onChanged();
                return this;
            }

            /**
             * <code>.proto.TestEnum testEnum = 16;</code>
             */
            public Builder clearTestEnum()
            {
                testEnum = 0;
                onChanged();
                return this;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            @Override
            public boolean hasSubMessage()
            {
                return subMessageBuilder != null || subMessage != null;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            @Override
            public SubMessage getSubMessage()
            {
                if (subMessageBuilder == null) {
                    return subMessage == null ? SubMessage.getDefaultInstance() : subMessage;
                }
                else {
                    return subMessageBuilder.getMessage();
                }
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            public Builder setSubMessage(SubMessage value)
            {
                if (subMessageBuilder == null) {
                    if (value == null) {
                        throw new NullPointerException();
                    }
                    subMessage = value;
                    onChanged();
                }
                else {
                    subMessageBuilder.setMessage(value);
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            public Builder setSubMessage(
                    SubMessage.Builder builderForValue)
            {
                if (subMessageBuilder == null) {
                    subMessage = builderForValue.build();
                    onChanged();
                }
                else {
                    subMessageBuilder.setMessage(builderForValue.build());
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            public Builder mergeSubMessage(SubMessage value)
            {
                if (subMessageBuilder == null) {
                    if (subMessage != null) {
                        subMessage =
                                SubMessage.newBuilder(subMessage).mergeFrom(value).buildPartial();
                    }
                    else {
                        subMessage = value;
                    }
                    onChanged();
                }
                else {
                    subMessageBuilder.mergeFrom(value);
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            public Builder clearSubMessage()
            {
                if (subMessageBuilder == null) {
                    subMessage = null;
                    onChanged();
                }
                else {
                    subMessage = null;
                    subMessageBuilder = null;
                }

                return this;
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            public SubMessage.Builder getSubMessageBuilder()
            {
                onChanged();
                return getSubMessageFieldBuilder().getBuilder();
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            @Override
            public SubMessageOrBuilder getSubMessageOrBuilder()
            {
                if (subMessageBuilder != null) {
                    return subMessageBuilder.getMessageOrBuilder();
                }
                else {
                    return subMessage == null ?
                            SubMessage.getDefaultInstance() : subMessage;
                }
            }

            /**
             * <code>.proto.SubMessage subMessage = 17;</code>
             */
            private com.google.protobuf.SingleFieldBuilderV3<
                    SubMessage, SubMessage.Builder, SubMessageOrBuilder>
                    getSubMessageFieldBuilder()
            {
                if (subMessageBuilder == null) {
                    subMessageBuilder = new com.google.protobuf.SingleFieldBuilderV3<
                            SubMessage, SubMessage.Builder, SubMessageOrBuilder>(
                            getSubMessage(),
                            getParentForChildren(),
                            isClean());
                    subMessage = null;
                }
                return subMessageBuilder;
            }

            private void ensureRepeatedFieldIsMutable()
            {
                if (!((bitField0 & 0x00020000) == 0x00020000)) {
                    repeatedField = new com.google.protobuf.LazyStringArrayList(repeatedField);
                    bitField0 |= 0x00020000;
                }
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            @Override
            public com.google.protobuf.ProtocolStringList getRepeatedFieldList()
            {
                return repeatedField.getUnmodifiableView();
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            @Override
            public int getRepeatedFieldCount()
            {
                return repeatedField.size();
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            @Override
            public String getRepeatedField(int index)
            {
                return repeatedField.get(index);
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            @Override
            public com.google.protobuf.ByteString getRepeatedFieldBytes(int index)
            {
                return repeatedField.getByteString(index);
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            public Builder setRepeatedField(
                    int index, String value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureRepeatedFieldIsMutable();
                repeatedField.set(index, value);
                onChanged();
                return this;
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            public Builder addRepeatedField(
                    String value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }
                ensureRepeatedFieldIsMutable();
                repeatedField.add(value);
                onChanged();
                return this;
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            public Builder addAllRepeatedField(
                    Iterable<String> values)
            {
                ensureRepeatedFieldIsMutable();
                com.google.protobuf.AbstractMessageLite.Builder.addAll(
                        values, repeatedField);
                onChanged();
                return this;
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            public Builder clearRepeatedField()
            {
                repeatedField = com.google.protobuf.LazyStringArrayList.EMPTY;
                bitField0 = (bitField0 & ~0x00020000);
                onChanged();
                return this;
            }

            /**
             * <code>repeated string repeatedField = 18;</code>
             */
            public Builder addRepeatedFieldBytes(
                    com.google.protobuf.ByteString value)
            {
                if (value == null) {
                    throw new NullPointerException();
                }
                checkByteStringIsUtf8(value);
                ensureRepeatedFieldIsMutable();
                repeatedField.add(value);
                onChanged();
                return this;
            }

            private com.google.protobuf.MapField<String, Double> internalGetMapField()
            {
                if (mapField == null) {
                    return com.google.protobuf.MapField.emptyMapField(
                            MapFieldDefaultEntryHolder.defaultEntry);
                }
                return mapField;
            }

            private com.google.protobuf.MapField<String, Double> internalGetMutableMapField()
            {
                onChanged();
                if (mapField == null) {
                    mapField = com.google.protobuf.MapField.newMapField(
                            MapFieldDefaultEntryHolder.defaultEntry);
                }
                if (!mapField.isMutable()) {
                    mapField = mapField.copy();
                }
                return mapField;
            }

            @Override
            public int getMapFieldCount()
            {
                return internalGetMapField().getMap().size();
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */
            @Override
            public boolean containsMapField(
                    String key)
            {
                if (key == null) {
                    throw new NullPointerException();
                }
                return internalGetMapField().getMap().containsKey(key);
            }

            /**
             * Use {@link #getMapFieldMap()} instead.
             */
            @Override
            @Deprecated
            public java.util.Map<String, Double> getMapField()
            {
                return getMapFieldMap();
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */
            @Override
            public java.util.Map<String, Double> getMapFieldMap()
            {
                return internalGetMapField().getMap();
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */
            @Override
            public double getMapFieldOrDefault(
                    String key,
                    double defaultValue)
            {
                if (key == null) {
                    throw new NullPointerException();
                }
                java.util.Map<String, Double> map =
                        internalGetMapField().getMap();
                return map.containsKey(key) ? map.get(key) : defaultValue;
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */
            @Override
            public double getMapFieldOrThrow(
                    String key)
            {
                if (key == null) {
                    throw new NullPointerException();
                }
                java.util.Map<String, Double> map =
                        internalGetMapField().getMap();
                if (!map.containsKey(key)) {
                    throw new IllegalArgumentException();
                }
                return map.get(key);
            }

            public Builder clearMapField()
            {
                internalGetMutableMapField().getMutableMap()
                        .clear();
                return this;
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */

            public Builder removeMapField(
                    String key)
            {
                if (key == null) {
                    throw new NullPointerException();
                }
                internalGetMutableMapField().getMutableMap()
                        .remove(key);
                return this;
            }

            /**
             * Use alternate mutation accessors instead.
             */
            @Deprecated
            public java.util.Map<String, Double> getMutableMapField()
            {
                return internalGetMutableMapField().getMutableMap();
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */
            public Builder putMapField(
                    String key,
                    double value)
            {
                if (key == null) {
                    throw new NullPointerException();
                }

                internalGetMutableMapField().getMutableMap()
                        .put(key, value);
                return this;
            }

            /**
             * <code>map&lt;string, double&gt; mapField = 19;</code>
             */

            public Builder putAllMapField(
                    java.util.Map<String, Double> values)
            {
                internalGetMutableMapField().getMutableMap()
                        .putAll(values);
                return this;
            }

            @Override
            public Builder setUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields)
            {
                return super.setUnknownFieldsProto3(unknownFields);
            }

            @Override
            public Builder mergeUnknownFields(
                    final com.google.protobuf.UnknownFieldSet unknownFields)
            {
                return super.mergeUnknownFields(unknownFields);
            }
            // @@protoc_insertion_point(builder_scope:proto.TestMessage)
        }
    }

    // @@protoc_insertion_point(outer_class_scope)
}
