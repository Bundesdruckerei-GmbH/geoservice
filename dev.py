#!/usr/bin/env python

import json
import os
import os.path
import subprocess
import sys
from typing import Optional, Dict


APP_NAME: str = 'geoservice'


def abspath(*args):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), *args))


def load_env():
    default_config = {
        "GEOSERVICE_DEBUG": "True",
        "GEOSERVICE_SECRET_KEY": "topsecret",
        "FLASK_DEBUG": "1",
        "FLASK_APP": f"{APP_NAME}:app",
    }
    try:
        with open(abspath("env.json"), "r") as fp:
            env_config = json.load(fp)
            default_config.update(env_config)
            return default_config
    except Exception as exc:
        print("env.json not usable: {}".format(exc))
        return {}


def command_flask(*args):
    """runs flask, inside the venv"""
    shell(["flask"] + list(args))


def command_build():
    """builds the application, so that it can be dockerized"""
    version = os.environ.get("HASH", "dev")

    with open(abspath("geoservice", "buildinfo.py"), "w") as fp:
        fp.write("version = {!r}\n".format(version))

    command_test()


def command_exec(*args):
    """runs a shell command, with the path set to include venv"""
    shell(list(args))


def command_test(*args):
    """run all the unittests not marked as expensive"""
    shell(
        ["pytest", "-vv", "tests"] + list(args),
        forced_env_variables={
            'GEOSERVICE_DATABASE_TYPE': 'sqlite',
            'GEOSERVICE_DATABASE_PATH': ':memory:',
        })


def command_help():
    """displays the help message"""
    print("usage: build <command> [<args>...]")
    print("")
    print("commands:")
    width = max(len(name) for name in commands.keys())
    for name, func in commands.items():
        summary = (func.__doc__ or "").split("\n")[0].strip()
        print("  %s - %s" % (name.ljust(width), summary))


class ShellError(RuntimeError):
    pass


def shell(args, load_env_json=True, capture_output=False, forced_env_variables: Optional[Dict] = None):
    allenv = {}
    allenv.update(os.environ)
    if load_env_json:
        allenv.update(load_env())
    if forced_env_variables:
        allenv.update(forced_env_variables)

    allenv.update(
        {
            "PYTHONPATH": os.pathsep.join(
                [abspath(), os.environ.get("PYTHONPATH", "")]
            ),
            "PATH": os.pathsep.join(
                [
                    abspath("venv", "Scripts"),
                    abspath("venv", "bin"),
                    abspath("node_modules", ".bin"),
                ]
                + [os.environ["PATH"]]
            ),
        }
    )

    exe = find_exe(args[0], allenv)

    try:
        proc = subprocess.run(
            [exe] + args[1:],
            check=True,
            env=allenv,
            capture_output=capture_output,
        )
        return proc.stdout
    except Exception as exc:
        raise ShellError(exc)


def find_exe(name, env):
    for p in env["PATH"].split(os.pathsep):
        if p.strip():
            for ext in env.get("PATHEXT", "").split(";"):
                fullpath = os.path.join(p, name + ext)
                if os.path.isfile(fullpath):
                    return fullpath
    raise Exception("no executable {} found".format(name))


commands = dict(
    (name[len("command_"):], func)
    for name, func in globals().items()
    if name.startswith("command_")
)


def main(args):
    try:
        if not args or args[0] not in commands:
            command_help()
        else:
            commands[args[0]](*args[1:])
    except ShellError as err:
        print(err)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
