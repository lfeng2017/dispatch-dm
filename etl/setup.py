# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import os
import os.path as op
import subprocess

from dispatchETL import main as app

setup(name=app.APP_NAME,

      version=app.VERSION,

      url='git@git.yongche.org:lujin/dispatch-modeling.git',

      license='MIT',

      author='lujin',

      author_email='lujin@yongche.com',

      description='Manage configuration files',

      packages=find_packages(exclude=['tests']),

      long_description=open('README.md').read(),

      zip_safe=False,

      setup_requires=['cement>=2.10.2'],

      test_suite='nose.collector')

if __name__ == "__main__":
    BASE_DIR = op.dirname(op.abspath(__file__))
    print BASE_DIR

    libs_path = BASE_DIR + "/dispatchETL/libs/"
    shell = "rm -fr {}".format(libs_path)
    print shell
    subprocess.call(shell, shell=True)

    requires_path = op.join(BASE_DIR, "requirements.txt")
    shell = "pip install -r {requires_path} -t {libs_path}".format(requires_path=requires_path, libs_path=libs_path)
    print shell
    subprocess.call(shell, shell=True)

    dist = BASE_DIR + "/dist/libs.zip"
    shell = "rm -f {dist} ".format(dist=dist)
    print shell
    subprocess.call(shell, shell=True)

    shell = "cd {libs} && zip -r {dist} .".format(dist=dist, libs=libs_path)
    print shell
    subprocess.call(shell, shell=True)

    print "depandencies pack to libs.zip in {}".format(dist)
