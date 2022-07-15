"""
Entry point for the cli if invoked with python -m simeon
"""
import os
import sys


# Remove '' and current working directory from the first entry
# of sys.path, if present to avoid using current directory
# when invoked as python -m pip <command>
if sys.path[0] in ("", os.getcwd()):
    sys.path.pop(0)

if __name__ == '__main__':
    from simeon.scripts.simeon import main as _main
    sys.exit(_main())
