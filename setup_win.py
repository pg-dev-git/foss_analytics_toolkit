# A very simple setup script to test adding summary data stream to an MSI file.
#
# hello.py is a very simple 'Hello, world' type script which also displays the
# environment in which the script runs
#
# Run the build process by running the command 'python setup.py bdist_msi'


from cx_Freeze import setup, Executable

executables = [Executable("TCRM_Toolkit.py")]

bdist_msi_options = {
    "summary_data": {
        "author": "Pedro Gagliardi",
        "comments": "TCRM Toolkit",
        "keywords": "Salesforce Tableau CRM",
    },
}

setup(
    name="tcrm_toolkit",
    version="0.1b0",
    description="TCRM Toolkit v0.1-beta",
    executables=executables,
    options={
        "build_exe": {"excludes": ["tkinter"]},
        "bdist_msi": bdist_msi_options,
    },
)
