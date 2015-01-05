import setuptools

setuptools.setup(
    description='An execution engine for async tasks',
    author='Bhupendra Singh',
    url='https://github.com/bhsinghgit/shepherd',
    author_email='bhsingh@gmail.com',
    version='0.37',
    install_requires=['flask', 'pymysql', 'gunicorn'],
    scripts=['bin/shepherd', 'bin/shepherd_api'],
    name='shepherd'
)
