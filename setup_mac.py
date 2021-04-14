from cx_Freeze import setup, Executable

options = {"build_exe": {"excludes": ["tkinter"]}}

executables = [
    Executable("TCRM_Toolkit.py"),
]

setup(
    name="tcrm_toolkit",
    version="0.1",
    description="tcrm_toolkit",
    executables=executables,
    options=options,
)
