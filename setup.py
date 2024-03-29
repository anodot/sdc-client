from setuptools import setup, find_packages

setup(
    name='sdc-client',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    version='1.2.1',
    install_requires=[
      'requests==2.25.0',
      'inject==4.3.1',
    ],
)
