#! /usr/bin/env python
#
# Copyright (C) 2016 S3IT, University of Zurich
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# make coding more python3-ish, must be the first statement
from __future__ import (absolute_import, division, print_function)


## module doc and other metadata
"""
Simulate TissueMaps I/O workload against different types of
storage backends.
"""
__docformat__ = 'reStructuredText'
__author__ = ('Riccardo Murri <riccardo.murri@gmail.com>')


## imports and other dependencies
from abc import ABCMeta, abstractmethod
from collections import namedtuple
import functools
import logging
import math
import os
import sys

from click import argument, command, group, option, echo

from gc3libs.quantity import Memory


## constants and defaults

class const:
    """A namespace for constant and default values."""

    logfmt = "%(asctime)s [%(processName)s/%(process)d %(funcName)s:%(lineno)d] %(levelname)s: %(message)s"
    loglevel = logging.DEBUG


## aux functions

def _setup_logging():
    try:
        import coloredlogs
        coloredlogs.install(
            fmt=const.logfmt,
            level=const.loglevel
        )
    except ImportError:
        logging.basicConfig(
            format=const.logfmt,
            level=const.loglevel
        )


def _get_payload(payload):
    try:
        try:
            size = int(payload)
            logging.warning(
                "No unit specified for payload size `%d`,"
                " assuming *bytes*", size)
        except (ValueError, TypeError):
            size = Memory(payload).amount(unit=Memory.B, conv=int)
        logging.debug(
            "Reading %d bytes of random data"
            " from `/dev/urandom` into memory...", size)
        with open('/dev/urandom', 'rb') as data:
            return data.read(size)
    except (ValueError, TypeError):
        logging.debug("Reading contents of file `%s` into memory...", payload)
        with open(payload, 'rb') as data:
            return data.read()


#
# Storage backends
#

_storage_backend = {}

def register_storage(cls, names):
    for name in names:
        _storage_backend[name] = cls
        logging.debug(
            "Using class '%s' for storage backend '%s' ...",
            cls.__name__, name)


def storage(*names):
    """
    Class decorator to register a storage backend.
    """
    return functools.partial(register_storage, names=names)


def make_storage(kind, *args, **kwargs):
    return _storage_backend[kind](*args, **kwargs)


class Storage(object):
    __metaclass__ = ABCMeta

    """
    Abstraction over the I/O operations that can be performed.
    Method names are loosely patterned after HTTP verbs of
    corresponding meaning.

    This is only an abstract class defining the interface; concrete
    classes should then provide the actual implementation methods.
    """

    @abstractmethod
    def __init__(self, uri):
        """
        Initialize the storage with a root URI.
        """
        pass

    @abstractmethod
    def get(self, location):
        """
        Return the (whole) data stored at `location`.

        Interpretation of the `location` parameter is entirely
        dependent on the concrete implementation class.
        """
        pass

    @abstractmethod
    def info(self, location):
        """
        Return metadata associated to the content of `location`.

        Interpretation of the `location` parameter is entirely
        dependent on the concrete implementation class.

        :return: A `Storage.info_result`:class: instance.
        """
        pass

    # this seemingly pointless class definition has the sole purpose
    # of attaching a docstring to a `namedtuple`
    class info_result(namedtuple('info_result', 'size')):
        """
        Metadata associated to a storage location.

        Namely, the available fields are:

        - *size*: Size of the associated data, encoded as a
          `gc3libs.quantity.Memory` object.
        """
        pass

    @abstractmethod
    def put(self, location, data):
        """
        Write the contents of the `data` string into `location`.
        Data already stored at `location` is overwritten and cannot be
        recovered.

        Interpretation of the `location` parameter is entirely
        dependent on the concrete implementation class.
        """
        pass


@storage('filesystem', 'fs', 'file')
class FilesystemStorage(Storage):
    """
    Implement I/O operations on the filesystem.

    A `location` in this context is defined as a filesystem path.
    """

    def __init__(self, uri):
        self.rootdir = uri
        # create output directory
        if not os.path.exists(self.rootdir):
            logging.info("Creating directory path '%s' ...", self.rootdir)
            os.makedirs(self.rootdir)

    def _get_absolute_location(self, loc):
        """
        Return absolute path to location `loc` *within* the root directory.
        """
        if not loc.startswith(self.rootdir):
            return os.path.join(self.rootdir,
                                (loc if loc[0] != '/' else loc[1:]))

    def get(self, location):
        location = self._get_absolute_location(location)
        with open(location, 'rb') as fd:
            return fd.read()

    def info(self, location):
        location = self._get_absolute_location(location)
        md = os.stat(location)
        return self.info_result(size=Memory(md.st_size, unit=Memory.B))

    def put(self, location, data):
        location = self._get_absolute_location(location)
        with open(location, 'wb') as fd:
            fd.write(data)


#
# Command-line interface definition
#

@group()
def cli():
    pass

@cli.command()
@argument("storage")
@argument("rootdir")
@argument("numfiles")
@argument("payload")  # Path to a template file or size of the random data to be generated
@option("--jobs", "-j",
        default=1, metavar='NUM',
        help="Allow NUM simultaneous writers.")
def create(storage, rootdir, numfiles, payload, jobs=1):
    """\
    Write fake payload in a given location.

    Fill the given location ROOTDIR with NUMFILES identical files.
    Argument PAYLOAD is either the path of a template file, which will
    be copied all over the place, or the size of random data to be
    stored into each of the (identical) files.

    First argument STORAGE chooses the storage backend type, which in
    turn determines how the root location specifier is interpreted.
    """
    _setup_logging()
    try:
        numfiles = int(numfiles)
        if numfiles < 1:
            raise ValueError()
    except ValueError, TypeError:
        logging.fatal("Argument NUMFILES must be a positive integer number.")
        sys.exit(1)
    data = _get_payload(payload)
    # create identical output files
    if jobs != 1:
        raise NotImplementedError("Multiple concurrent jobs are not yet supported.")
    storage = make_storage(storage, rootdir)
    prec = 1 + int(math.log(numfiles, 10))
    for n in xrange(numfiles):
        outfile = ("data{{n:0{p}d}}".format(p=prec).format(n=n))
        storage.put(outfile, data)
    logging.info("All done.")


@cli.command()
def selftest():
    """Run unit tests."""
    try:
        import pytest
        pytest.main(['-v', '--doctest-modules', __file__])
    except ImportError:
        # no `py.test`, but `doctest` is always available
        import doctest
        doctest.testmod(name="iosim",
                        optionflags=doctest.NORMALIZE_WHITESPACE)


if __name__ == '__main__':
    cli()
