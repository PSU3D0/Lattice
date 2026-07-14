#!/usr/bin/env python3
import pathlib
import sys

EXPECTED_FUNCTIONS = {
    "lf_alloc": ([0x7F], [0x7F]),
    "lf_transform": ([0x7F, 0x7F], [0x7F]),
    "lf_output_ptr": ([], [0x7F]),
    "lf_output_len": ([], [0x7F]),
}
EXPECTED_EXPORTS = {"memory", *EXPECTED_FUNCTIONS, "__data_end", "__heap_base"}
ALLOWED_CUSTOM_SECTIONS = {"name", "producers"}


class Reader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def byte(self):
        if self.offset >= len(self.data):
            raise ValueError("unexpected end of wasm")
        value = self.data[self.offset]
        self.offset += 1
        return value

    def bytes(self, length):
        end = self.offset + length
        if end > len(self.data):
            raise ValueError("unexpected end of wasm")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def uleb(self):
        value = 0
        shift = 0
        while True:
            byte = self.byte()
            value |= (byte & 0x7F) << shift
            if byte & 0x80 == 0:
                return value
            shift += 7
            if shift > 63:
                raise ValueError("invalid LEB128")

    def vector(self, read_item):
        return [read_item() for _ in range(self.uleb())]

    def name(self):
        return self.bytes(self.uleb()).decode("utf-8")


def limits(reader):
    flags = reader.uleb()
    if flags not in (0, 1):
        raise ValueError(f"unsupported limits flags {flags}")
    minimum = reader.uleb()
    maximum = reader.uleb() if flags & 1 else None
    return minimum, maximum


def inspect(path):
    reader = Reader(pathlib.Path(path).read_bytes())
    if reader.bytes(8) != b"\x00asm\x01\x00\x00\x00":
        raise ValueError("not a core wasm v1 module")

    types = []
    function_types = []
    tables = []
    memories = []
    globals_ = []
    exports = {}
    custom = []
    has_start = False
    import_count = 0

    while reader.offset < len(reader.data):
        section_id = reader.byte()
        payload = Reader(reader.bytes(reader.uleb()))
        if section_id == 0:
            custom.append(payload.name())
        elif section_id == 1:
            def read_type():
                if payload.byte() != 0x60:
                    raise ValueError("non-function type")
                params = payload.vector(payload.byte)
                results = payload.vector(payload.byte)
                return params, results
            types = payload.vector(read_type)
        elif section_id == 2:
            import_count = payload.uleb()
            if import_count:
                raise ValueError(f"expected zero imports, found {import_count}")
        elif section_id == 3:
            function_types = payload.vector(payload.uleb)
        elif section_id == 4:
            def read_table():
                reference_type = payload.byte()
                minimum, maximum = limits(payload)
                return reference_type, minimum, maximum
            tables = payload.vector(read_table)
        elif section_id == 5:
            memories = payload.vector(lambda: limits(payload))
        elif section_id == 6:
            def read_global():
                value_type = payload.byte()
                mutable = payload.byte()
                while payload.byte() != 0x0B:
                    pass
                return value_type, mutable
            globals_ = payload.vector(read_global)
        elif section_id == 7:
            def read_export():
                name = payload.name()
                kind = payload.byte()
                index = payload.uleb()
                if name in exports:
                    raise ValueError(f"duplicate export {name}")
                exports[name] = (kind, index)
            payload.vector(read_export)
        elif section_id == 8:
            has_start = True

    unknown_custom = set(custom) - ALLOWED_CUSTOM_SECTIONS
    if unknown_custom:
        raise ValueError(f"unapproved custom sections: {sorted(unknown_custom)}")
    if has_start:
        raise ValueError("start function is forbidden")
    if len(memories) != 1 or memories[0][1] != 2048 or memories[0][0] > 2048:
        raise ValueError(f"expected one bounded memory32 with max 2048 pages, found {memories}")
    if len(tables) > 1 or any(t[0] != 0x70 or t[2] is None or t[2] > 4096 for t in tables):
        raise ValueError(f"table profile exceeded: {tables}")
    if set(exports) != EXPECTED_EXPORTS:
        raise ValueError(f"unexpected exports: {sorted(exports)}")
    if exports["memory"] != (2, 0):
        raise ValueError("memory export does not select the sole memory")

    for name, expected_type in EXPECTED_FUNCTIONS.items():
        kind, index = exports[name]
        if kind != 0 or index >= len(function_types):
            raise ValueError(f"{name} is not a defined function")
        actual_type = types[function_types[index]]
        if actual_type != expected_type:
            raise ValueError(f"{name} has type {actual_type}, expected {expected_type}")

    for name in ("__data_end", "__heap_base"):
        kind, index = exports[name]
        if kind != 3 or index >= len(globals_):
            raise ValueError(f"{name} is not a defined global")
        if globals_[index] != (0x7F, 0):
            raise ValueError(f"{name} must be immutable i32")

    return custom, memories[0], tables


def main():
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} MODULE.wasm")
    custom, memory, tables = inspect(sys.argv[1])
    print("admission: zero imports; no start; exact lattice.transform.v1 exports")
    print(f"memory: min={memory[0]} pages max={memory[1]} pages")
    print(f"tables: {tables}")
    print(f"custom sections: {custom}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, UnicodeError, ValueError) as error:
        print(f"inspection failed: {error}", file=sys.stderr)
        raise SystemExit(1)
