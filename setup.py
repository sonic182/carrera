"""Setup module."""

from setuptools import setup
try:
    from pip._internal.req import parse_requirements
except ImportError:
    from pip.req import parse_requirements


def requirements(filename):
    """Parse requirements from requirements.txt."""
    reqs = parse_requirements(filename, session=False)
    return [str(req.req) for req in reqs]


setup(
    name='carrera',
    version='0.0.1',
    description='Concurrency Framework',
    author='Johanderson Mogollon',
    author_email='johanderson@mogollon.com.ve',
    license='MIT',
    packages=['carrera'],
    setup_requires=['pytest-runner'],
    test_requires=['pytest'],
    install_requires=requirements('./requirements.txt'),
    extras_require={
        'test': requirements('./test-requirements.txt')
    }
)
