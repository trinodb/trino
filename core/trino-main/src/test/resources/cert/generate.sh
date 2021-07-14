#!/bin/sh

set -eux

script_dir=$(CDPATH="" cd -- "$(dirname -- "$0")" && pwd)
outfile="$1"
shift
if [ -z "$outfile" ]; then
    outfile="$script_dir/localhost.pem"
fi

tempdir=$(mktemp -d)
cp "$script_dir/localhost.conf" "$tempdir/localhost.conf"
printf "%s\n" "$@" >> "$tempdir/localhost.conf"
openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout "$tempdir/localhost.key" -days 3560 -out "$tempdir/localhost.crt" -config "$tempdir/localhost.conf"
cat "$tempdir/localhost.crt" "$tempdir/localhost.key" > "$outfile"
rm -rf "$tempdir"
