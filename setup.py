from setuptools import setup

setup(
    name='cenRequest',
    version='0.0.1',
    description='Request data from CEN API',
    license='MIT',
    packages=['cenRequest'],
    author='Diego Guiraldes',
    author_email='dguiraldes@gmail.com',
    keywords=['CEN'],
    url='https://github.com/dguiraldes/cenRequest',
    install_requires=["pandas","requests"]
)