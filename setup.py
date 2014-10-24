try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    description='An execution engine for async tasks',
    author='Bhupendra Singh',
    url='https://github.com/bhsinghgit/shepherd',
    author_email='bhsingh@gmail.com',
    version='0.3',
    packages=['shepherd'],
    scripts=['bin/shepherd', 'bin/cli', 'bin/worker'],
    py_modules=['logger', 'worker', 'sheepdog', 'sheep'],
    name='shepherd'
)
