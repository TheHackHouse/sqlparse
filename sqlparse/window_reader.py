from typing import Iterator


class WindowReader:

    def __init__(
        self, file, window_size=1000000, base_position=10000, buffer_size=10000
    ):
        self._window_size = window_size
        self._base_position = base_position
        self._buffer_size = buffer_size

        self._cursor_offset = -self._base_position

        self._file_handle = file
        self.window = self._file_handle.read(self._window_size)

        self._eof = False

    def _flush_buffer(self):
        next_chars = self._file_handle.read(self._buffer_size)
        if next_chars is None or len(next_chars) < self._buffer_size:
            self._eof = True

        if next_chars is not None:
            self.window = self.window[self._buffer_size :] + next_chars
            self._cursor_offset = 0

    def __iter__(self) -> Iterator[tuple[int, str]]:
        while self._base_position + self._cursor_offset < len(self.window):
            if not self._eof and self._cursor_offset >= self._buffer_size:
                self._flush_buffer()

            current_pos = self._base_position + self._cursor_offset
            yield (current_pos, self.window[current_pos])

            self._cursor_offset += 1
