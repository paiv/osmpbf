#!/usr/bin/env python
import fnmatch
import io
import re
import struct
import sys
import zlib
from pathlib import Path


_VERBOSE = False


def trace(*args, **kwargs):
    if _VERBOSE: print(*args, **kwargs, flush=True, file=sys.stderr)


def pbuf_read_varint(file):
    r = 0
    t = 1
    l = 0
    i = 0
    while t:
        s = file.read(1)
        if not s: raise Exception('unexpected EOF')
        l += 1
        x = s[0]
        r |= (x & 0x7f) << i
        i += 7
        t = x & 0x80
    return r,l


def pbuf_read_varsint(file):
    v,l = pbuf_read_varint(file)
    v = -(v >> 1) if (v & 1) else (v >> 1)
    return (v, l)


def pbuf_read_str(file):
    n,l = pbuf_read_varint(file)
    if not n:
        return (b'', l)
    val = b''
    while n > 0:
        s = file.read(n)
        if not s: break
        val += s
        n -= len(s)
    return (s, len(val) + l)


def pbuf_read_tag(file):
    v,l = pbuf_read_varint(file)
    f = v >> 3
    t = v & 7
    return (f,t,l)


def pbuf_read_tagval(file):
    f,t,n = pbuf_read_tag(file)
    match t:
        case 0:
            i,l = pbuf_read_varint(file)
            # print('tag', f, i, file=sys.stderr)
            return (f, i, n+l)
        case 2:
            s,l = pbuf_read_str(file)
            # print('tag', f, f'(len {l})', s[:10], file=sys.stderr)
            return (f, s, n+l)
        case _:
            raise Exception(f'unhandled field {f!r} type {t!r}')


def pbuf_parse_tagvals(data):
    file = io.BytesIO(data)
    size = len(data)
    while size > 0:
        t,v,l = pbuf_read_tagval(file)
        size -= l
        yield (t,v)
    assert size == 0


def pbuf_parse_packed(data):
    file = io.BytesIO(data)
    size = len(data)
    vals = list()
    while size > 0:
        v,l = pbuf_read_varint(file)
        size -= l
        vals.append(v)
    assert size == 0
    return vals


def pbuf_parse_packed_deltas(data):
    size = len(data)
    if not size: return list()
    file = io.BytesIO(data)
    v,l = pbuf_read_varsint(file)
    size -= l
    vals = [v]
    while size > 0:
        d,l = pbuf_read_varsint(file)
        size -= l
        v += d
        vals.append(v)
    assert size == 0
    return vals


def pbf_read(file):
    def read_message(file, size):
        vals = list()
        while size > 0:
            t,v,l = pbuf_read_tagval(file)
            size -= l
            vals.append((t,v))
        assert size == 0
        return dict(vals)
    def read_header(file):
        s = file.read(4)
        if not s: return
        size, = struct.unpack('>I', s)
        return read_message(file, size)
    while True:
        h = read_header(file)
        if not h: break
        t = h.get(1)
        n = h.get(3, 0)
        m = read_message(file, n)
        if (v := m.get(1)):
            yield (t, m)
        else:
            n = m.get(2, 0)
            if (v := m.get(3)):
                v = zlib.decompress(v)
                assert len(v) == n
                yield (t, v)
            else:
                s = set(m.keys()) - {2}
                raise Exception(f'unahndled encoding {s}')


def osm_read_header_block(data):
    file = io.BytesIO(data)
    size = len(data)
    vals = list()
    while size > 0:
        t,v,l = pbuf_read_tagval(file)
        size -= l
        if t == 1:
            v = list(pbuf_parse_tagvals(v))
        vals.append((t,v))
    return vals


def osm_read_data_block(data):
    names = None
    for t,v in pbuf_parse_tagvals(data):
        if t == 2:
            v = osm_read_group(v, names)
            yield (t,v)
        elif t == 1:
            names = osm_read_stringtable(v)


def osm_read_stringtable(data):
    names = list()
    for t,s in pbuf_parse_tagvals(data):
        if t == 1:
            names.append(s.decode())
        else:
            raise Exception(f'unhandled tag {t} in string table')
    return names


def osm_read_group(data, names):
    vals = list()
    for t,v in pbuf_parse_tagvals(data):
        if t == 1:
            v = osm_read_node(v, names)
            vals.append(v)
        elif t == 2:
            vs = osm_read_dense_nodes(v, names)
            vals.extend(vs)
    return vals


def osm_read_node(data, names):
    vals = list()
    for t,v in pbuf_parse_tagvals(data):
        vals.append((t, v))
    return vals


def osm_read_dense_nodes(data, names):
    for t,v in pbuf_parse_tagvals(data):
        if t == 1:
            ids = pbuf_parse_packed_deltas(v)
        elif t == 8:
            lats = pbuf_parse_packed_deltas(v)
        elif t == 9:
            lons = pbuf_parse_packed_deltas(v)
        elif t == 10:
            packed = pbuf_parse_packed(v)
            kvs = list()
            gs = iter(packed)
            node = list()
            for i in gs:
                if i == 0:
                    kvs.append(node)
                    node = list()
                else:
                    k = names[i]
                    i = next(gs)
                    v = names[i]
                    node.append((k,v))
    assert len(ids) == len(lats) == len(lons) == len(kvs)
    return list(zip(ids, lats, lons, kvs))


def osm_enumerate_nodes(data):
    for t,v in osm_read_data_block(data):
        if t == 2: # primitivegroup
            yield from v


def _compile_query(query):
    qs = list()
    for s in query:
        ps = s.split('=', maxsplit=1)
        if len(ps) == 1:
            k, = ps
            v = '*'
        else:
            k,v = ps
        rk = re.compile(fnmatch.translate(k))
        rv = re.compile(fnmatch.translate(v))
        qs.append((rk, rv))
    return qs


def _node_match(node, query):
    match = list()
    seen = set()
    nid = str(node[0])
    for q, (rk,rv) in enumerate(query):
        if rk.match('id') and rv.match(nid):
            seen.add(q)
            match.append(('id', nid))
            break
    if (kvs := node[3]) is not None:
        for k,v in kvs:
            for q, (rk,rv) in enumerate(query):
                if rk.match(k) and rv.match(v):
                    seen.add(q)
                    match.append((k,v))
                    break
    if len(seen) == len(query):
        return match


def main(args):
    filename = Path(args.file)
    query = _compile_query(args.query)
    count = 0
    with filename.open('rb') as fp:
        for h,m in pbf_read(fp):
            if h == b'OSMHeader':
                h = osm_read_header_block(m)
                trace(h)
            elif h == b'OSMData':
                for node in osm_enumerate_nodes(m):
                    if (m := _node_match(node, query)) is not None:
                        count += 1
                        if args.value_only:
                            for k,v in m:
                                print(v)
                        elif args.full_node:
                            print()
                            print(f"'id'={node[0]!r}")
                            for k,v in (node[3] or list()):
                                print(f'{k!r}={v!r}')
                        else:
                            for k,v in m:
                                print(f'{k!r}={v!r}')

                        if (args.limit is not None) and (count >= args.limit):
                            return


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='OSM PBF reader')
    parser.add_argument('file', help='.osm.pbf file to read')
    parser.add_argument('query', nargs='+', help='node filter; key=value, * for any match')
    parser.add_argument('-i', '--full-node', action='store_true', help='output matching node')
    parser.add_argument('-q', '--value-only', action='store_true', help='output only matching value')
    parser.add_argument('-n', '--limit', type=int, help='limit to first N matches')
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    args = parser.parse_args()
    if args.verbose:
        _VERBOSE = True
    main(args)
