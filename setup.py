#!/usr/bin/env python

from setuptools import setup

setup(name='tap-timebutler',
      version="2.0.7",
      description='Singer.io tap for extracting data from the timebutler api',
      author='Taikonauten GmbH & Co. KG',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_timebutler'],
      install_requires=[
          'singer-python==5.9.0',
          'requests==2.31.0',
          'pendulum==1.2.0',
          'backoff==1.8.0',
          'pandas==1.2.4'
      ],
      entry_points='''
          [console_scripts]
          tap-timebutler=tap_timebutler:main
      ''',
      packages=['tap_timebutler'],
      package_data = {
          'tap_timebutler/schemas': [
              "absences.json",
              "users.json",
              "holidayentitlement.json",
              "workdays.json",
              "worktime.json",
              "projects.json",
              "services.json",
          ],
      },
      include_package_data=True,
)
