from cx_Freeze import setup, Executable

options = {"build_exe": {"excludes": ["tkinter"]}}

executables = [
    Executable("TCRM_Toolkit.py"),
]

setup(
    name="tcrm_toolkit",
    version="0.2b2",
    description="TCRM Toolkit v0.1-beta2",
    executables=executables,
    options=options,
)
