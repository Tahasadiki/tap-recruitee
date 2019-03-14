#!/usr/bin/env python

from setuptools import setup

''' add this when installing this tap alone
install_requires=[
    'attrs==18.1.0',
    'backoff==1.3.2',
    'python-dateutil==2.7.3',
    'requests==2.20.0',
    'singer-python==5.3.3',
],
'''

setup(name='tap-recruitee',
      version='1.0.0',
      description='Singer.io tap for extracting data from Recruitee API',
      author='Sadiki Taha',
      url='https://github.com/Tahasadiki',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_recruitee'],
      install_requires = [],
      entry_points="""
      [console_scripts]
      tap-recruitee=tap_recruitee:main
      """,
      packages=["tap_recruitee", "tap_recruitee.filters"],
      package_data={
          "schemas": ["tap_recruitee/schemas/*.json"]
      },
      include_package_data=True,
      )
