/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.cli;

import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.infer.OptionDescription;
import io.starburst.schema.discovery.options.GeneralOptions;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.TextTable;
import picocli.CommandLine.IHelpSectionRenderer;

import java.util.Arrays;
import java.util.stream.Stream;

public class OptionsHelp
        implements IHelpSectionRenderer
{
    @Override
    public String render(Help help)
    {
        StringBuilder str = new StringBuilder();

        str.append("\n").append("=============== Options ===============").append("\n\n");

        str.append("Note: table specific options can be set by prefixing with \"<schema>.<table>.\"\n\n");

        str.append("===============\nOption Defaults\n===============").append("\n");

        TextTable textTable = TextTable.forColumnWidths(help.colorScheme(), 120);
        CsvOptions.standard().forEach((option, value) -> textTable.addRowValues(option + " = " + value));
        textTable.toString(str);

        outputOptions(str, help, "================\nStandard Options\n================", GeneralOptions.class);
        outputOptions(str, help, "================\nCSV/TEXT Options\n================", CsvOptions.class);
        return str.toString();
    }

    private void outputOptions(StringBuilder str, Help help, String heading, Class<?> clazz)
    {
        str.append("\n");
        str.append(heading);
        str.append("\n");
        TextTable textTable = TextTable.forColumnWidths(help.colorScheme(), 30, 90);
        outputOptions(textTable, clazz);
        textTable.toString(str);
    }

    private void outputOptions(TextTable textTable, Class<?> clazz)
    {
        Stream.of(clazz.getDeclaredFields()).forEach(field -> {
            OptionDescription description = field.getAnnotation(OptionDescription.class);
            if (description != null) {
                try {
                    String value = description.value();
                    Object[] enumConstants = description.enums().getEnumConstants();
                    if (enumConstants != null) {
                        value += " " + Arrays.toString(enumConstants);
                    }
                    textTable.addRowValues(field.get(null).toString(), value);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
