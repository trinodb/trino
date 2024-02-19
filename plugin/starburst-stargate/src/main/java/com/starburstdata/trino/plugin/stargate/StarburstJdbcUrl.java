/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.base.Joiner;
import io.trino.jdbc.TrinoDriverUri;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({METHOD, TYPE_USE})
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = {StarburstJdbcUrl.StringJdbcUrlValidator.class, StarburstJdbcUrl.OptionalJdbcUrlValidator.class})
public @interface StarburstJdbcUrl
{
    enum Presence
    {
        PRESENT,
        ABSENT,
        OPTIONAL
    }

    String message() default "Invalid Starburst JDBC URL";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    Presence catalog() default Presence.OPTIONAL;

    abstract class AbstractJdbcUrlValidator<T>
            implements ConstraintValidator<StarburstJdbcUrl, T>
    {
        private Presence catalog;

        @Override
        public void initialize(StarburstJdbcUrl constraintAnnotation)
        {
            this.catalog = constraintAnnotation.catalog();
        }

        @Override
        public boolean isValid(T value, ConstraintValidatorContext context)
        {
            String jdbcUrl = getJdbcUrl(value);
            if (jdbcUrl == null) {
                // Not our responsibility
                return true;
            }

            TrinoDriverUri driverUri;
            try {
                driverUri = TrinoDriverUri.create(jdbcUrl, getUserProperties());
            }
            catch (SQLException e) {
                String defaultMessage = "Invalid Starburst JDBC URL, sample format: jdbc:trino://localhost:8080";
                if (catalog == Presence.PRESENT) {
                    defaultMessage += "/catalog_name";
                }
                context.buildConstraintViolationWithTemplate(Joiner.on(": ").skipNulls()
                                .join(defaultMessage, e.getMessage()))
                        .addPropertyNode("isValid")
                        .addConstraintViolation();
                return false;
            }
            if (catalog == Presence.ABSENT && driverUri.getCatalog().isPresent()) {
                context.buildConstraintViolationWithTemplate("Invalid Starburst JDBC URL: catalog and/or schema must not be provided")
                        .addPropertyNode("isValid")
                        .addConstraintViolation();
                return false;
            }
            if (catalog == Presence.PRESENT && driverUri.getCatalog().isEmpty()) {
                context.buildConstraintViolationWithTemplate("Invalid Starburst JDBC URL: catalog is not provided")
                        .addPropertyNode("isValid")
                        .addConstraintViolation();
                return false;
            }
            if (driverUri.getSchema().isPresent()) {
                context.buildConstraintViolationWithTemplate("Invalid Starburst JDBC URL: schema must not be provided")
                        .addPropertyNode("isValid")
                        .addConstraintViolation();
                return false;
            }
            return true;
        }

        protected abstract String getJdbcUrl(T value);

        private static Properties getUserProperties()
        {
            // connection user is required by TrinoDriverUri validations
            Properties properties = new Properties();
            properties.put("user", "trino");
            return properties;
        }
    }

    class StringJdbcUrlValidator
            extends AbstractJdbcUrlValidator<String>
    {
        @Override
        protected String getJdbcUrl(String value)
        {
            return value;
        }
    }

    class OptionalJdbcUrlValidator
            extends AbstractJdbcUrlValidator<Optional<?>>
    {
        @Override
        protected String getJdbcUrl(Optional<?> value)
        {
            return (String) value
                    .filter(String.class::isInstance)
                    .orElse(null);
        }
    }
}
