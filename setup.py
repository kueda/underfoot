"""Install some OS-agnostic dependencies"""
from subprocess import run
import os


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


if __name__ == "__main__":
    shell_cmd(
        ["pip", "install", "-r", "requirements.txt"],
        msg="INSTALLING PYTHON PACKAGES..."
    )
    setup_e00conv()
