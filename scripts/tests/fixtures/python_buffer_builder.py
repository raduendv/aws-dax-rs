class _UniffiRustBufferBuilder:
    """
    Helper for structured writing of bytes into a _UniffiRustBuffer.
    """

    def __init__(self):
        self.rbuf = _UniffiRustBuffer.alloc(16)
        self.rbuf.len = 0

    def finalize(self):
        rbuf = self.rbuf
        self.rbuf = None
        return rbuf

    def discard(self):
        if self.rbuf is not None:
            rbuf = self.finalize()
            rbuf.free()

    @contextlib.contextmanager
    def _reserve(self, num_bytes):
        if self.rbuf.len + num_bytes > self.rbuf.capacity:
            self.rbuf = _UniffiRustBuffer.reserve(self.rbuf, num_bytes)
        yield None
        self.rbuf.len += num_bytes

    def _pack_into(self, size, format, value):
        with self._reserve(size):
            # XXX TODO: I feel like I should be able to use `struct.pack_into` here but can't figure it out.
            for i, byte in enumerate(struct.pack(format, value)):
                self.rbuf.data[self.rbuf.len + i] = byte

    def write(self, value):
        with self._reserve(len(value)):
            for i, byte in enumerate(value):
                self.rbuf.data[self.rbuf.len + i] = byte

    def write_i8(self, v):
        self._pack_into(1, ">b", v)

    def write_u8(self, v):
        self._pack_into(1, ">B", v)

    def write_i16(self, v):
        self._pack_into(2, ">h", v)

    def write_u16(self, v):
        self._pack_into(2, ">H", v)

    def write_i32(self, v):
        self._pack_into(4, ">i", v)

    def write_u32(self, v):
        self._pack_into(4, ">I", v)

    def write_i64(self, v):
        self._pack_into(8, ">q", v)

    def write_u64(self, v):
        self._pack_into(8, ">Q", v)

    def write_float(self, v):
        self._pack_into(4, ">f", v)

    def write_double(self, v):
        self._pack_into(8, ">d", v)

    def write_c_size_t(self, v):
        self._pack_into(ctypes.sizeof(ctypes.c_size_t) , "@N", v)
