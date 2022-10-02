import setuptools
from setuptools import find_packages

reqs = [
    # "Deprecated"
]


def find_dbacademy_packages():
    packages = find_packages(where="src")
    print("-"*80)
    print(packages)
    print("-"*80)
    return packages


setuptools.setup(
    name="dbacademy-helper",
    version="0.0.0",
    install_requires=reqs,
    package_dir={"dbacademy_helper": "src/dbacademy_helper"},
    packages=find_dbacademy_packages()
)
