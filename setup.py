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
    packages=['pm4pymdl', 'pm4pymdl.algo', 'pm4pymdl.algo.mvp', 'pm4pymdl.algo.mvp.utils',
              'pm4pymdl.algo.mvp.discovery', 'pm4pymdl.algo.mvp.discovery.versions', 'pm4pymdl.algo.mvp.projection',
              'pm4pymdl.algo.mvp.projection.versions', 'pm4pymdl.algo.mvp.gen_framework',
              'pm4pymdl.algo.mvp.gen_framework.util', 'pm4pymdl.algo.mvp.gen_framework.models',
              'pm4pymdl.algo.mvp.gen_framework.edge_freq', 'pm4pymdl.algo.mvp.gen_framework.node_freq',
              'pm4pymdl.algo.mvp.gen_framework.rel_events', 'pm4pymdl.algo.mvp.gen_framework.rel_activities',
              'pm4pymdl.algo.mvp.gen_framework2', 'pm4pymdl.algo.mvp.gen_framework2.md_ts',
              'pm4pymdl.algo.mvp.gen_framework2.md_dfg', 'pm4pymdl.algo.mvp.gen_framework2.md_petri_alpha',
              'pm4pymdl.algo.mvp.gen_framework2.md_process_trees',
              'pm4pymdl.algo.mvp.gen_framework2.md_petri_inductive', 'pm4pymdl.algo.mvp.gen_framework3',
              'pm4pymdl.algo.mvp.gen_framework3.versions_discovery',
              'pm4pymdl.algo.mvp.gen_framework3.versions_conformance', 'pm4pymdl.algo.mvp.gen_framework4',
              'pm4pymdl.algo.mvp.gen_framework4.versions_discovery',
              'pm4pymdl.algo.mvp.gen_framework4.versions_conformance', 'pm4pymdl.algo.mvp.get_logs_and_replay',
              'pm4pymdl.algo.mvp.get_logs_and_replay.versions', 'pm4pymdl.algo.bindings', 'pm4pymdl.algo.simulation',
              'pm4pymdl.util', 'pm4pymdl.util.parquet_exporter', 'pm4pymdl.util.parquet_exporter.versions',
              'pm4pymdl.util.parquet_importer', 'pm4pymdl.util.parquet_importer.versions', 'pm4pymdl.objects',
              'pm4pymdl.objects.jmd', 'pm4pymdl.objects.jmd.exporter', 'pm4pymdl.objects.jmd.importer',
              'pm4pymdl.objects.mdl', 'pm4pymdl.objects.mdl.exporter', 'pm4pymdl.objects.mdl.importer',
              'pm4pymdl.objects.xoc', 'pm4pymdl.objects.xoc.exporter', 'pm4pymdl.objects.xoc.exporter.versions',
              'pm4pymdl.objects.xoc.importer', 'pm4pymdl.objects.xoc.importer.versions', 'pm4pymdl.objects.sqlite',
              'pm4pymdl.objects.sqlite.exporter', 'pm4pymdl.objects.sqlite.importer', 'pm4pymdl.objects.openslex',
              'pm4pymdl.objects.openslex.importer', 'pm4pymdl.objects.openslex.importer.versions',
              'pm4pymdl.visualization', 'pm4pymdl.visualization.mvp', 'pm4pymdl.visualization.mvp.versions',
              'pm4pymdl.visualization.mvp.gen_framework', 'pm4pymdl.visualization.mvp.gen_framework.versions',
              'pm4pymdl.visualization.mvp.gen_framework2', 'pm4pymdl.visualization.mvp.gen_framework2.versions',
              'pm4pymdl.visualization.mvp.gen_framework3', 'pm4pymdl.visualization.mvp.gen_framework4',
              'pm4pymdl.visualization.petrinet', 'pm4pymdl.visualization.petrinet.versions'],
    url='http://www.pm4py.org',
    license='GPL 3.0',
    install_requires=[
        "pm4py==1.5.2.3",
        "pyarrow==1.0.1",
        "Flask",
        "flask-cors",
        "setuptools",
        "frozendict"
    ],
    project_urls={
        'Documentation': 'http://pm4py.pads.rwth-aachen.de/documentation/',
        'Source': 'https://github.com/pm4py/pm4py-source',
        'Tracker': 'https://github.com/pm4py/pm4py-source/issues',
    }
)
