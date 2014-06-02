import multiprocessing # to avoid a bug of python, http://bugs.python.org/issue15881#msg170215
from setuptools import setup, find_packages

setup(
    name = 'ptshell',
    version = '0.0.1',
    author = 'Tao Peng',
    author_email = 'pengtao.arthur@gmail.com',
    packages = find_packages(),
    zip_safe = True,
    test_suite = 'nose.collector',
    tests_require = ['nose']
)
