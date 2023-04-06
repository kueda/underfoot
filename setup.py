"""Install some OS-agnostic dependencies"""
from subprocess import run
import os
import shutil

def shell_cmd(cmd, msg=None):
    """Execute a shell command"""
    print(f"\n{msg}")
    run(cmd, check=True)


def setup_e00conv():
    """Downloading and install tool for converting e00 files"""
    if os.path.isfile("bin/e00compr/e00conv"):
        return
    print("\nINSTALLING E00CONV")
    work_path = "bin"
    if not os.path.isdir("bin"):
        os.makedirs(work_path)
    os.chdir(work_path)
    basename = "e00compr-1.0.1"
    run(
        ["curl", "-OL", f"http://avce00.maptools.org/dl/{basename}.tar.gz"],
        check=True
    )
    run(["tar", "xzvf", f"{basename}.tar.gz"], check=True)
    run(["ln", "-s", basename, "e00compr"], check=True)
    os.chdir(basename)
    run("make", check=True)
    os.chdir("../../")


def setup_imposm():
    """Download imposm3 binary for importing OpenStreetMap data"""
    if os.path.isfile("bin/imposm/imposm"):
        return
    work_path = "bin"
    if not os.path.isdir("bin"):
        os.makedirs(work_path)
    imposm_path = os.path.join(work_path, "imposm")
    if os.path.isdir(imposm_path):
        shutil.rmtree(imposm_path)
    elif os.path.isfile(imposm_path):
        os.remove(imposm_path)
    os.chdir(work_path)
    basename = "imposm-0.11.1-linux-x86-64"
    run([
        "curl",
        "-OL",
        f"https://github.com/omniscale/imposm3/releases/download/v0.11.1/{basename}.tar.gz"
    ], check=True)
    run(["tar", "xzvf", f"{basename}.tar.gz"], check=True)
    run(["ln", "-s", os.path.join(basename, "imposm"), "imposm"], check=True)
    os.chdir("../")

if __name__ == "__main__":
    setup_e00conv()
    setup_imposm()
