#!/usr/bin/env python

import argparse


def generate(factors, formats, tables):
    for format in formats:
        for factor in factors:
            new_schema = "tpch_" + factor + "_" + format
            source_schema = "tpch." + factor
            print(
                "CREATE SCHEMA IF NOT EXISTS hive.{};".format(
                    new_schema,
                )
            )
            for table in tables:
                print(
                    'CREATE TABLE IF NOT EXISTS "hive"."{}"."{}" WITH (format = \'{}\') AS SELECT * FROM {}."{}";'.format(
                        new_schema, table, format, source_schema, table
                    )
                )


def main():
    parser = argparse.ArgumentParser(description="Generate test data.")
    parser.add_argument(
        "--factors",
        type=csvtype(["tiny", "sf1", "sf100", "sf300", "sf1000", "sf3000"]),
        default=["sf300", "sf1000", "sf3000"],
    )
    parser.add_argument(
        "--formats", type=csvtype(["orc", "text"]), default=["orc", "text"]
    )
    default_tables = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    parser.add_argument(
        "--tables", type=csvtype(default_tables), default=default_tables
    )

    args = parser.parse_args()
    generate(args.factors, args.formats, args.tables)


def csvtype(choices):
    """Return a function that splits and checks comma-separated values."""

    def splitarg(arg):
        values = arg.split(",")
        for value in values:
            if value not in choices:
                raise argparse.ArgumentTypeError(
                    "invalid choice: {!r} (choose from {})".format(
                        value, ", ".join(map(repr, choices))
                    )
                )
        return values

    return splitarg


if __name__ == "__main__":
    main()
