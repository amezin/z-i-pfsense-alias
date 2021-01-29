#!/usr/bin/env python3

import argparse
import csv
import ipaddress
import sys


csv.field_size_limit(sys.maxsize)


def merge(addrs):
    addrs = sorted(addrs)
    if not addrs:
        return []

    merged = []

    def add_merged(addr):
        while merged:
            prev = merged[-1]
            if addr.subnet_of(prev):
                return

            addr_super = addr.supernet()
            if tuple(addr_super.subnets()) != (prev, addr):
                break

            addr = addr_super
            del merged[-1]

        merged.append(addr)

    for addr in addrs:
        add_merged(addr)

    return merged


def run(dump_csv, output, output_v6):
    reader = csv.reader(dump_csv, delimiter=';')
    next(reader)  # "Updated on ..."

    addrs = set()

    for row in reader:
        for addr in row[0].split('|'):
            addr = addr.strip()
            if not addr:
                continue

            addr = ipaddress.ip_network(addr)
            if addr.is_multicast or addr.is_private or addr.is_unspecified or addr.is_reserved or addr.is_loopback or addr.is_link_local:
                continue

            addrs.add(addr)

    print('Total unique subnets:', len(addrs), file=sys.stderr)

    merged_v4 = merge(addr for addr in addrs if addr.version == 4)
    merged_v6 = merge(addr for addr in addrs if addr.version == 6)

    print('Merged:', len(merged_v4) + len(merged_v6), file=sys.stderr)
    print('Total addresses:', sum(net.num_addresses for net in merged_v4) + sum(net.num_addresses for net in merged_v6), file=sys.stderr)

    for net in merged_v4:
        print(net, file=output)

    if output_v6 is None:
        output_v6 = output

    for net in merged_v6:
        print(net, file=output_v6)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dump_csv', type=argparse.FileType('r', encoding='cp1251'))
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-6', '--output-v6', type=argparse.FileType('w'))
    run(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
