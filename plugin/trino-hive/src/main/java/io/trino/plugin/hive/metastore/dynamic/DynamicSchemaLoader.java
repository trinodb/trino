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
package io.trino.plugin.hive.metastore.dynamic;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.airlift.log.Logger;
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import io.trino.metastore.type.TypeInfo;
import io.trino.metastore.type.TypeInfoFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;

public final class DynamicSchemaLoader
{
    private static final Logger LOG = Logger.get(DynamicSchemaLoader.class);

    private DynamicSchemaLoader()
    {
    }

    public static Column fieldToColumn(FieldDescriptor fieldDescriptor)
    {
        String name = fieldDescriptor.getName().toLowerCase(ENGLISH);
        return new Column(name, HiveType.fromTypeInfo(getType(fieldDescriptor, ImmutableList.of(fieldDescriptor.getFullName()))), Optional.of(fieldDescriptor.getFullName()), Map.of());
    }

    private static TypeInfo getType(FieldDescriptor fieldDescriptor, List<String> path)
    {
        TypeInfo baseType = switch (fieldDescriptor.getJavaType()) {
            case BOOLEAN -> HiveType.HIVE_BOOLEAN.getTypeInfo();
            case INT -> HiveType.HIVE_INT.getTypeInfo();
            case LONG -> HiveType.HIVE_LONG.getTypeInfo();
            case FLOAT -> HiveType.HIVE_FLOAT.getTypeInfo();
            case DOUBLE -> HiveType.HIVE_DOUBLE.getTypeInfo();
            case BYTE_STRING -> HiveType.HIVE_BINARY.getTypeInfo();
            case STRING, ENUM -> HiveType.HIVE_STRING.getTypeInfo();
            case MESSAGE -> {
                List<String> names = new ArrayList<>();
                List<TypeInfo> typeInfos = new ArrayList<>();
                List<FieldDescriptor> innerDescriptors = fieldDescriptor.getMessageType().getFields();
                for (FieldDescriptor innerDescriptor : innerDescriptors) {
                    if (path.contains(innerDescriptor.getFullName())) {
                        LOG.warn("Descriptor recursion detected; omitting " + innerDescriptor.getFullName());
                        continue;
                    }
                    List<String> innerPath = new ArrayList<>(path);
                    innerPath.add(innerDescriptor.getFullName());

                    names.add(innerDescriptor.getName());
                    typeInfos.add(getType(innerDescriptor, ImmutableList.copyOf(innerPath)));
                }
                yield TypeInfoFactory.getStructTypeInfo(names, typeInfos);
            }
        };
        if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
            return TypeInfoFactory.getListTypeInfo(baseType);
        }
        return baseType;
    }
}
