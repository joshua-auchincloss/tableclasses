from os import getcwd
from pathlib import PosixPath
from sys import path

p = PosixPath(getcwd()) / "src"

path.append(str(p))
