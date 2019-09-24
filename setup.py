"""Setup module."""


import re
from setuptools import setup
from setuptools import find_packages

RGX = re.compile('([\w-]+==[\d.]+)')


def read_file(filename):
    """Read file correctly."""
    with open(filename) as _file:
        return _file.read().strip()


def requirements(filename):
    """Parse requirements from file."""
    return re.findall(RGX, read_file(filename)) or []


setup(
    name='carrera',
    version=read_file('VERSION'),
    description='Concurrency Framework',
    author='Johanderson Mogollon',
    author_email='johanderson@mogollon.com.ve',
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    license='MIT',
    classifiers=[
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    packages=find_packages(),
    install_requires=requirements('./requirements.txt'),
    extras_require={
        'test': requirements('./test-requirements.txt')
    }
)
