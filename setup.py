#!/usr/bin/env python

from distutils.core import setup

setup(name='omicidx',
      version='0.1.0',
      description='Tooling for parsing, databasing, and indexing omics database metadata',
      author='Sean Davis',
      author_email='seandavi@gmail.com',
      url='https://github.com/seandavi/omicidx',
      packages=['omicidx',
                'omicidx.scripts',
                'omicidx.sra'],
      install_requires = [
          "click"
      ],
      entry_points = {
          "console_scripts":[ 
              #"omicidx=omicidx.scripts.omicidx:omicidx_cli",
              "omicidx-cli=omicidx.scripts.cli:cli" 
          ]
      }
     )
