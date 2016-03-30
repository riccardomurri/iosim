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
from tempfile import NamedTemporaryFile
import time

from click import argument, command, group, option, echo, BadArgumentUsage

from gc3libs import create_engine, Application
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


def last(start, end, step=1):
    """
    Return the maximum number in range START:END:STEP.
    """
    rangelen = (end - start) // step
    top = start + (rangelen-1)*step
    assert top < end
    return top


def _parse_and_validate_arg(value, conv, valid, errmsg,
                            name='', errcls=BadArgumentUsage, do_exit=False):
    """
    Convert a string argument and validate result.

    Upon failure of the conversion or validation,
    raise a `click.BadArgumentUsage` exception or
    log error and exit program, depending on the value
    of the `do_exit` parameter.

    Arguments `conv` and `valid` should be 1-ary functions:

    * `conv` takes a string argument and returns the wanted result;
    * `valid` takes the converted argument and returns ``True`` or ``False``.

    Fourth argument `errmsg` is the error message to show when
    conversion or validation fails.  Optional argument `name`, if
    provided, is substituted into every occurrence of the string
    ``{name}`` in the error message text using the `.format()` method.
    """
    try:
        result = conv(value)
        if not valid(result):
            raise ValueError()
        else:
            return result
    except (ValueError, TypeError):
        msg = errmsg.format(arg=(" '" + name + "' ") if name else ' ')
        if do_exit:
            logging.fatal(msg)
            sys.exit(os.EX_USAGE)
        else:
            raise errcls(msg)


def nonnegative_int(value, name='', do_exit=False):
    """
    Convert to integer and check that the result is >=0.

    Upon failure of the conversion or validation,
    raise a `click.BadArgumentUsage` exception or
    log error and exit program, depending on the value
    of the `do_exit` parameter.

    Second argument `name`, if provided, is used in the error message
    text to name the quantity that failed conversion.
    """
    return _parse_and_validate_arg(
        value, int, (lambda arg: arg >= 0),
        errmsg="Argument{arg}must be a non-negative integer number.",
        name=name, do_exit=do_exit)


def positive_int(value, name='', do_exit=False):
    """
    Convert to integer and check that the result is >0.

    Upon failure of the conversion or validation,
    raise a `click.BadArgumentUsage` exception or
    log error and exit program, depending on the value
    of the `do_exit` parameter.

    Second argument `name`, if provided, is used in the error message
    text to name the quantity that failed conversion.
    """
    return _parse_and_validate_arg(
        value, int, (lambda arg: arg > 0),
        errmsg="Argument{arg}must be a positive integer number.",
        name=name, do_exit=do_exit)


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
            try:
                os.makedirs(self.rootdir)
            except IOError as err:
                if err.errno == 17:
                    # directory exists, might have been created by an
                    # independent process -- ignore
                    pass
                else:
                    raise

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
# GC3Pie interface
#

def run_jobs(jobs, argv, interval=1, max_concurrent=0):
    """
    Create and run jobs, each executing the command specified by `argv`.

    If any item in sequence `argv` is equal to the (single-character
    string) ``#``, it is substituted with the current job index.
    """
    engine = create_engine(max_in_flight=max_concurrent)
    for n in xrange(jobs):
        jobname = ('worker{n}'.format(n=n))
        job_argv = [(arg if arg != '#' else n) for arg in argv]
        engine.add(Application(
            job_argv,
            inputs=[],
            outputs=[],
            output_dir=os.path.join(os.getcwd(), jobname),
            stdout=(jobname + '.log'),
            join=True,
            jobname = jobname,
        ))
    # loop until all jobs are done
    stats = engine.stats()
    done = stats['TERMINATED']
    while done < jobs:
        time.sleep(interval)
        engine.progress()
        stats = engine.stats()
        done = stats['TERMINATED']
        logging.info(
            "%d jobs terminated (of which %d successfully),"
            " %d running, %d queued.",
            done, stats['ok'], stats['RUNNING'], stats['SUBMITTED'] + stats['NEW']
        )


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
@option("--work-dir", "-d",
        default=None, metavar='DIR',
        help=("Working directory for writing temporary files."
              " Must be visible to all worker processes."))
def create(storage, rootdir, numfiles, payload, jobs=1, work_dir=None):
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
    numfiles = positive_int(numfiles, 'NUMFILES')
    data = _get_payload(payload)

    prec = 1 + int(math.log(numfiles, 10))
    fmt = "data{{0:0{p}d}}".format(p=prec)

    # write payload data to a file in a shared directory
    if work_dir is None:
        work_dir = os.getcwd()
    with NamedTemporaryFile(prefix='payload.', dir=work_dir) as payload_file:
        payload_file.write(data)
        # must be careful to ensure that data is flushed out to the
        # filesystem otherwise workers running remotely might not see it
        payload_file.flush()
        os.fsync(payload_file)

        # create identical output files
        run_jobs(jobs, [os.path.realpath(sys.argv[0]), 'worker', 'wr',
                        storage, rootdir, fmt, '#', numfiles, jobs, payload_file.name])
    logging.info("All done.")


@cli.group()
def worker():
    """
    Low level actions, mainly for internal use.
    """
    pass


@worker.command()
@argument("storage")
@argument("rootdir")
@argument("pattern")
@argument("start")
@argument("end")
@argument("step")
@option("--md5",
        default=None, metavar='HASH',
        help=("Check that the MD5 checksum of each read file is exactly HASH."
              " (Must be a 32-digit hexadecimal string.)"))
def rd(storage, rootdir, pattern, start, end, step, md5=None):
    """
    Read a range of files and check intergrity.
    """
    _setup_logging()
    storage = make_storage(storage, rootdir)
    start = nonnegative_int(start, 'START')
    end = positive_int(end, 'END')
    step = positive_int(step, 'STEP')
    logging.info(
        "Will read %d files, names ranging from '%s' to '%s' with stepping %d",
        ((end - start) // step),  # total nr. of files
        pattern.format(start),
        pattern.format(last(start, end, step)),
        step)
    for n in xrange(start, end, step):
        infile = (pattern.format(n))
        data = storage.get(infile)
        if md5:
            hasher = hashlib.md5()
            hasher.update(data)
            digest = hasher.digest()
            if digest != md5:
                logging.error(
                    "Data in '%s' has MD5 digest '%s',"
                    " different from expected '%s'",
                    digest, md5)
    logging.info("All done.")


@worker.command()
@argument("storage")
@argument("rootdir")
@argument("pattern")
@argument("start")
@argument("end")
@argument("step")
@argument("payload")  # Path to a template file or size of the random data to be generated
def wr(storage, rootdir, pattern, start, end, step, payload):
    """
    Write a range of identical files.
    """
    _setup_logging()
    storage = make_storage(storage, rootdir)
    start = nonnegative_int(start, 'START')
    end = positive_int(end, 'END')
    step = positive_int(step, 'STEP')
    with open(payload, 'rb') as stream:
        logging.debug("Loading data payload from file '%s' ...", payload)
        data = stream.read()
    logging.debug("Using pattern '%s' for creating names.", pattern)
    logging.info(
        "Creating %d files, names ranging from '%s' to '%s' with stepping %d",
        ((end - start) // step),  # total nr. of files
        pattern.format(start),
        pattern.format(last(start, end, step)),
        step)
    for n in xrange(start, end, step):
        outfile = (pattern.format(n))
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
