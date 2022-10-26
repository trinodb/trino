#!/usr/bin/env python

import argparse


def generate(factors, formats, tables):
    for format in formats:
        for factor in factors:
            new_schema = "tpcds_" + factor + "_" + format
            source_schema = "tpcds." + factor
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
        type=csvtype(
            ["tiny", "sf1", "sf10", "sf30", "sf100", "sf300", "sf1000", "sf3000", "sf10000"]
        ),
        default=["sf10", "sf30", "sf100", "sf300", "sf1000", "sf3000", "sf10000"],
    )
    parser.add_argument("--formats", type=csvtype(["orc", "text"]), default=["orc"])
    default_tables = [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
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
