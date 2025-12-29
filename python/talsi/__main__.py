from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from talsi import Storage


class CLIError(Exception):
    pass


def list_namespaces(storage: Storage):
    for ns in storage.list_namespaces():
        print(ns)


def list_keys(storage: Storage, args: argparse.Namespace):
    if args.namespace:
        namespaces = [args.namespace]
    else:
        namespaces = storage.list_namespaces()

    for ns in namespaces:
        keys = storage.list_keys(ns)
        if args.namespace:
            for key in keys:
                print(key)
        else:
            for key in keys:
                print(f"{ns}\t{key}")


def _dump_value(value):
    if isinstance(value, (dict, list)):
        print(json.dumps(value, indent=2))
    elif isinstance(value, bytes):
        sys.stdout.buffer.write(value)
    else:
        print(value)


def get_value(storage: Storage, args: argparse.Namespace):
    if args.key:
        value = storage.get(args.namespace, args.key)
        if value is None:
            raise CLIError(f"Key '{args.key}' not found in namespace '{args.namespace}'")
        _dump_value(value)
    else:
        keys = storage.list_keys(args.namespace)
        items = storage.get_many(args.namespace, keys)
        for key, value in items.items():
            print(key, end="\t")
            _dump_value(value)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, help="Path to the Talsi database file")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to execute")

    subparsers.add_parser("list-namespaces", help="List all namespaces in the database")

    list_keys_parser = subparsers.add_parser(
        "list-keys",
        help="List keys in a namespace or all namespaces",
    )
    list_keys_parser.add_argument(
        "-n",
        "--namespace",
        help="Namespace to list keys from (if not specified, lists keys from all namespaces)",
    )

    get_parser = subparsers.add_parser("get", help="Get value(s) from a namespace")
    get_parser.add_argument("-n", "--namespace", required=True, help="Namespace to get values from")
    get_parser.add_argument(
        "-k",
        "--key",
        help="Key to get (if not specified, gets all keys in the namespace)",
    )

    args = parser.parse_args(args)

    from talsi import Storage

    try:
        with Storage(args.file) as storage:
            if args.command == "list-namespaces":
                list_namespaces(storage)
            elif args.command == "list-keys":
                list_keys(storage, args)
            elif args.command == "get":
                get_value(storage, args)
    except CLIError as ce:
        parser.error(str(ce))


if __name__ == "__main__":
    main()
