from io import TextIOBase
from pandas import pd


class NCBIDump(TextIOBase):
    def __init__(self, file: str):
        self._file = open(file)

    def readline(self, *args):
        line = self._file.readline(*args)
        return line[:-3]

    def read(self, *args):
        chunk = self._file.read(*args)
        return chunk.replace("\t|\n", "\n")

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._file.close()
