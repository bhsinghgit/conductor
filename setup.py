try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    description='An execution engine for async tasks',
    author='Bhupendra Singh',
    url='https://github.com/bhsinghgit/shepherd',
    author_email='bhsingh@gmail.com',
    version='0.1',
    packages=['shepherd'],
    scripts=['bin/notifier',
             'bin/launcher',
             'bin/worker',
             'bin/collector',
             'bin/shipper',
             'bin/cli'],
    name='shepherd'
)
