
from setuptools import setup

setup(
    name='kaminario_flocker_driver',
    version='1.10',
    description='Kanimario Storage Center Plugin for Flocker',
    license='Apache 2.0',
    classifiers=[
        'Development Status :: Basic',
        'Programming Language :: Python :: 2.7.11',
    ],
    install_requires=[
        'requests>=2.5.2',
        'six',
        'bitmath',
        'krest'],
    keywords='backend, plugin, flocker, docker, python',
    packages=['kaminario_flocker_driver', 'kaminario_flocker_driver/utils'],
    author='Calsoft',
    author_email='kaminario-flocker@calsoftinc.com',
    url='https://github.com/Kaminario/flocker-driver',
    zip_safe=False,
    download_url='',
)

