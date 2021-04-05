#!/usr/bin/env python3

import argparse
import concurrent.futures
import csv
import ipaddress
import logging
import socket
import sys
import urllib.parse


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


def resolve_dns(domain):
    addrs = set()

    try:
        for family, type, proto, canonname, sockaddr in socket.getaddrinfo(domain, 0):
            addrs.add(sockaddr[0])

    except Exception:
        logging.exception("Can't resolve %r", domain)

    return addrs


def iter_field(field):
    for item in field.split('|'):
        item = item.strip()
        if item:
            yield item


def run(dump_csv, output, output_v6, dns_jobs):
    reader = csv.reader(dump_csv, delimiter=';')
    next(reader)  # "Updated on ..."

    addrs = set()
    domains = set()

    def add_addr(addr):
        try:
            ipaddr = ipaddress.ip_network(addr)

        except Exception:
            logging.exception("Can't parse %r as IP", addr)
            return

        if ipaddr.is_multicast:
            logging.warning('%r is multicast, ignoring', ipaddr)
            return

        if ipaddr.is_private:
            logging.warning('%r is private, ignoring', ipaddr)
            return

        if ipaddr.is_unspecified:
            logging.warning('%r is unspecified, ignoring', ipaddr)
            return

        if ipaddr.is_reserved:
            logging.warning('%r is reserved, ignoring', ipaddr)
            return

        if ipaddr.is_loopback:
            logging.warning('%r is loopback, ignoring', ipaddr)
            return

        if ipaddr.is_link_local:
            logging.warning('%r is link-local, ignoring', ipaddr)
            return

        addrs.add(ipaddr)

    for row in reader:
        for addr in iter_field(row[0]):
            add_addr(addr)

        if not dns_jobs:
            continue

        for domain in iter_field(row[1]):
            if domain.startswith('*.'):
                domain = domain[2:]

            if domain:
                domains.add(domain)

        for url in iter_field(row[2]):
            try:
                domains.add(urllib.parse.urlsplit(url, scheme='http').hostname)

            except Exception:
                logging.exception("Can't parse %r as URL", url)

    logging.info('Total unique subnets: %d', len(addrs))

    if dns_jobs:
        logging.info('Total hostnames/domains: %d', len(domains))

        with concurrent.futures.ThreadPoolExecutor(max_workers=dns_jobs) as executor:
            for resolved_addrs in executor.map(resolve_dns, domains):
                for addr in resolved_addrs:
                    add_addr(addr)

        logging.info('Total unique subnets after DNS resolution: %d', len(addrs))

    merged_v4 = merge(addr for addr in addrs if addr.version == 4)
    merged_v6 = merge(addr for addr in addrs if addr.version == 6)

    logging.info('Merged subnets: %d', len(merged_v4) + len(merged_v6))
    logging.info('Total addresses: %d', sum(net.num_addresses for net in merged_v4) + sum(net.num_addresses for net in merged_v6))

    for net in merged_v4:
        print(net, file=output)

    if output_v6 is None:
        output_v6 = output
    else:
        output.close()

    for net in merged_v6:
        print(net, file=output_v6)

    output_v6.close()


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('dump_csv', type=argparse.FileType('r', encoding='cp1251'))
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('-6', '--output-v6', type=argparse.FileType('w'))
    parser.add_argument('-j', '--dns-jobs', type=int, default=4)
    run(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
