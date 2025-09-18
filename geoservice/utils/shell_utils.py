import os


def find_exe(name, env):
    for p in env["PATH"].split(os.pathsep):
        if p.strip():
            for ext in env.get("PATHEXT", "").split(";"):
                fullpath = os.path.join(p, name + ext)
                if os.path.isfile(fullpath):
                    return fullpath
    raise Exception("no executable {} found".format(name))