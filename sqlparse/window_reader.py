class WindowReader:

    def __init__(
        self,
        file,
        window_size=500_000_000,
        base_position=100_000,
        buffer_size=100_000_000,
    ):
        self._window_size = window_size
        self._base = base_position
        self._buffer_size = buffer_size

        self._cursor = -self._base

        self._file_handle = file
        self.window = self._file_handle.read(self._window_size)

        if len(self.window) < self._window_size:
            self._eof = True
        else:
            self._eof = False

    def __iter__(self):
        return self

    def __next__(self) -> tuple[int, str]:
        current_pos = self._base + self._cursor

        if current_pos >= self._window_size or (
            self._eof and current_pos >= len(self.window)
        ):
            raise StopIteration

        if not self._eof and self._cursor >= self._buffer_size:
            next_chars = self._file_handle.read(self._buffer_size)
            next_chars_len = len(next_chars)

            if next_chars_len < self._buffer_size:
                self._eof = True

            if next_chars_len != 0:
                self.window = self.window[self._buffer_size :] + next_chars
                self._cursor = 0

        self._cursor += 1
        return (current_pos, self.window[current_pos])

    def consume(self, n):
        if self._eof:
            self._cursor += n
            return

        move_forward = self._cursor + n

        while not self._eof and move_forward >= self._buffer_size:
            next_chars = self._file_handle.read(self._buffer_size)

            if len(next_chars) < self._buffer_size:
                self._eof = True

            if next_chars:
                self.window = self.window[self._buffer_size :] + next_chars

            move_forward -= self._buffer_size

        self._cursor = move_forward
