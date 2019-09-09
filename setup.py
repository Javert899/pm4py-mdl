from os.path import dirname, join

from setuptools import setup

import pm4pymdl

def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()

setup(
    name=pm4pymdl.__name__,
    version=pm4pymdl.__version__,
    description=pm4pymdl.__doc__.strip(),
    long_description=read_file('README.md'),
    author=pm4pymdl.__author__,
    author_email=pm4pymdl.__author_email__,
    py_modules=[pm4pymdl.__name__],
    include_package_data=True,
    packages=['pm4pymdl', 'pm4pymdl.algo', 'pm4pymdl.objects', 'pm4pymdl.objects.mdl', 'pm4pymdl.objects.mdl.exporter',
              'pm4pymdl.objects.mdl.importer', 'pm4pymdl.objects.xoc', 'pm4pymdl.objects.xoc.exporter',
              'pm4pymdl.objects.xoc.exporter.versions', 'pm4pymdl.objects.xoc.importer',
              'pm4pymdl.objects.xoc.importer.versions', 'pm4pymdl.objects.openslex',
              'pm4pymdl.objects.openslex.importer', 'pm4pymdl.objects.openslex.importer.versions',
              'pm4pymdl.visualization', 'pm4pymdl.visualization.mvp', 'pm4pymdl.visualization.mvp.gen_framework',
              'pm4pymdl.visualization.mvp.gen_framework.versions'],
    url='http://www.pm4py.org',
    license='GPL 3.0',
    install_requires=[
        "pm4py",
        "pyarrow"
    ],
    project_urls={
        'Documentation': 'http://pm4py.pads.rwth-aachen.de/documentation/',
        'Source': 'https://github.com/pm4py/pm4py-source',
        'Tracker': 'https://github.com/pm4py/pm4py-source/issues',
    }
)
