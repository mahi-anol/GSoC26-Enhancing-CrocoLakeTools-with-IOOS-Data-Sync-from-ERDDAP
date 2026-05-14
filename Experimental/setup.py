from setuptools import setup, find_packages

setup(
    name="ioos_downloader",
    version="0.1.0",
    description=(
        "ERDDAP-based dataset downloader (prototype for GSoC 2026 "
        "CrocoLakeTools IOOS sync project)."
    ),
    author="Mahi Sarwar Anol",
    author_email="anol.mahi@gmail.com",
    packages=find_packages(exclude=["tests", "scripts"]),
    install_requires=[
        "erddapy>=2.0.0",
        "requests>=2.25.0",
        "pandas>=1.3.0",
        "tqdm>=4.60.0",
        "pyarrow>=10.0.0",
        "pytest",
        "setuptools"
    ],
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "download_ioos_gliders=scripts.download_ioos_gliders:main",
        ],
    },
)