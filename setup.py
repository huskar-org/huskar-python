import os
import re
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


def _get_version():
    v_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'huskar_sdk_v2', '__init__.py')
    ver_info_str = re.compile(r".*version = \((.*?)\)", re.S). \
        match(open(v_file_path).read()).group(1)
    return re.sub(r'(\'|"|\s+)', '', ver_info_str).replace(',', '.')


# package meta info
NAME = "huskar-sdk-v2"
VERSION = _get_version()
DESCRIPTION = ""
AUTHOR = "Haochuan Guo"
AUTHOR_EMAIL = "guohaochuan@gmail.com"
LICENSE = "MIT"
URL = "https://github.com/huskar-org/huskar-python"
KEYWORDS = "huskar"
REQUIREMENTS = [
    "simplejson==3.7.3",
    "blinker==1.3",
    "gevent>=1.0.1,<1.3.0",
    "atomicfile==1.0",
    "requests",
]

# package contents
PACKAGES = find_packages(
    exclude=['tests.*', 'tests', 'examples.*', 'examples',
             'dev_requirements.txt'])

here = os.path.abspath(os.path.dirname(__file__))


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


tests_require = ["pytest==3.2.2",
                 "pylint==1.6.4",
                 "pytest-cov==2.5.1",
                 "pytest-xdist==1.20.0",
                 "pytest-mock==1.6.2",
                 "mock==2.0.0"]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    install_requires=REQUIREMENTS,
    tests_require=tests_require,
    cmdclass={'test': PyTest},
    extras_require={'test': tests_require,
                    'bootstrap': ['kazoo'],
                    'doc': ['Sphinx==1.3.1',
                            'sphinx-rtd-theme==0.1.8']},
    license=LICENSE,
    url=URL,
    keywords=KEYWORDS,
    packages=PACKAGES,
    zip_safe=False,
    classifiers=[],
)
