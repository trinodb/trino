/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.type.TypeInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveTypeSerialization
{
    private final ObjectMapper mapper = new ObjectMapperProvider().get();

    @Test
    public void testSerialization()
            throws IllegalAccessException
    {
        for (Field field : HiveTypes.class.getFields()) {
            if (field.getType().equals(TypeInfo.class) && isPublic(field.getModifiers()) && isStatic(field.getModifiers()) && isFinal(field.getModifiers())) {
                TypeInfo typeInfo = (TypeInfo) field.get(null);
                testSerialization(typeInfo);
                testSerialization(HiveTypes.arrayType(typeInfo));
                testSerialization(HiveTypes.mapType(typeInfo, typeInfo));
                testSerialization(HiveTypes.structType(ImmutableList.of("x"), ImmutableList.of(typeInfo)));
            }
        }
    }

    private void testSerialization(TypeInfo typeInfo)
    {
        try {
            HiveType hiveType = new HiveType(typeInfo);
            String json = mapper.writeValueAsString(hiveType);
            HiveType deserialized = mapper.readValue(json, HiveType.class);
            assertThat(deserialized).isEqualTo(hiveType);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }
}
