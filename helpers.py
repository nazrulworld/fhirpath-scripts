# -*- coding: utf-8 -*-
# @Date    : 2018-05-28 12:51:52
# @Author  : Md Nazrul Islam (email2nazrul@gmail.com)
# @Link    : http://nazrul.me/
# @Version : $Id$
# All imports here
import argparse
import enum
import importlib
import inspect
import sys
import typing
from collections import deque


__author__ = "Md Nazrul Islam (email2nazrul@gmail.com)"


class FHIR_RELEASES(enum.Enum):
    """ """
    R4 = "R4"
    STU3 = "STU3"
    DSTU2 = "DSTU2"


class FHIR_INFO(enum.Enum):
    """ """

    BASE_URL = "http://hl7.org/fhir"
    EXT = ".zip"
    FHIR_DEFINITION = "definitions.json" + EXT
    FHIR_EXAMPLE = "examples-json" + EXT
    FHIR_SCHEMAS = "fhir.schema.json" + EXT


ArgumentParserType = typing.NewType("ArgumentParser", argparse.ArgumentParser)
ArgumentParserNamespaceType = typing.NewType(
    "ArgumentParserNamespaceType", argparse.Namespace
)


def get_parser() -> ArgumentParserType:
    """"""
    prog = "python -m cli" if sys.argv[0].endswith("__main__.py") else "cli"

    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        "worker",
        action="store",
        help="name of worker that will do " "execution. i.e downloader or crawler",
    )
    return parser


def setup_parser(parser: ArgumentParserType) -> ArgumentParserNamespaceType:
    """
    :param parser:
    :return:
    """

    parser.add_argument(
        "-S",
        "--source-path",
        action="store",
        dest="source_path",
        help="File path of resources",
        default=None,
    )

    parser.add_argument(
        "-U",
        "--source-url",
        action="store",
        dest="source_url",
        help="Any source URL of resources",
        default=None,
    )

    parser.add_argument(
        "-D",
        "--destination-dir",
        action="store",
        dest="destination_dir",
        help="Destination directory, where output files will " "be stored",
        default=None,
    )

    parser.add_argument(
        "-O",
        "--output-stream",
        action="store_true",
        dest="output_stream",
        help="Flag for print the output instead of storing on " "filesytem",
        default=False,
    )

    parser.add_argument(
        "-o",
        "--offline",
        action="store_true",
        dest="offline",
        help="Flag for looing local cache instead content " "from online",
        default=False,
    )

    parser.add_argument(
        "-v", "--verbosity-level", dest="verbosity_level", action="count"
    )

    parser.add_argument(
        "-R", "--releases", dest="releases", nargs="+", choices=["R4", "STU3", "DSTU2"]
    )
    return parser.parse_args()


def cmd(func: typing.Callable[[], typing.Any]) -> typing.Callable:
    """
    :param func:
    :return:
    """

    def new_func(*args, **kwargs):

        new_kwargs = {}
        fn_args = deque()

        for param in inspect.signature(func).parameters.values():

            if param.kind.name in (
                "POSITIONAL_ONLY",
                "POSITIONAL_OR_KEYWORD",
                "KEYWORD_ONLY",
            ):
                fn_args.append(param.name)

        for i, a in enumerate(fn_args):
            # should not pass an argument more than once
            if i >= len(args) and a in kwargs:
                new_kwargs[a] = kwargs.get(a)

        return func(*args, **new_kwargs)

    return new_func


async def run_cmd(func: typing.Callable[[], typing.Any], args: []) -> int:
    """
    """
    if isinstance(func, (str, bytes)):

        func = globals().get(func)

        if callable(func) or inspect.isfunction(func) or inspect.ismethod(func):
            out = await func(**args)

    elif isinstance(func, (tuple, list)):
        out = await globals().get(func)(*args.get(func), **args)

    elif callable(func):
        out = await func(**args)

    return out


async def get_function(args: typing.Dict) -> typing.Callable:
    """
    """
    func = args.get("worker")

    if not func.startswith("cli_"):

        _callable = globals().get("%s_%s" % ("cli", func))

        if _callable:
            return _callable

    try:
        parts = func.split(".")

        m = importlib.import_module("%s" % parts[0])
        try:
            fn = parts[1]
        except IndexError:
            fn = "main"

        return getattr(m, fn)

    except ImportError as exc:
        sys.stderr.write(str(exc))
        raise


async def run(parsed_args: typing.Dict) -> int:

    fn = await get_function(parsed_args)

    try:
        return await run_cmd(fn, parsed_args)

    except UnboundLocalError:
        # invalid command provided
        sys.stderr.write(
            "Invalid worker name! ~{0}~ is not found!".format(parsed_args.get("worker"))
        )
        return 1
