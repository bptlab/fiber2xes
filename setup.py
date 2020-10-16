from setuptools import setup, find_packages  # type: ignore


requirements = [
    'sqlalchemy>=1.3.3',
    'sqlalchemy-hana>=0.3.0',
    'opyenxes>=0.3.0',
    'pandas>=0.25.3',
    'pyspark>=2.4.5'
]


setup(
    name='fiber2xes',
    version='0.9',
    description='Fiber 2 Event-Log (in xes) converter',
    url='https://gitlab.hpi.de/pm1920/fiber2xes',
    author='Arne Boockmeyer, Finn Klessascheck, Francois Peverali, Martin Meier, Simon Siegert, Tom Lichtenstein',
    author_email='arne.boockmeyer@student.hpi.de, finn.klessascheck@student.hpi.de, francois.peverali@student.hpi.de, martin.meier@student.hpi.de, simon.siegert@student.hpi.de, tom.lichtenstein@student.hpi.de',
    keywords='fiber fiber2xes xes',
    packages=find_packages(),
    install_requires=requirements,
    extras_require={
        'dev': [
            'typing-extensions>=3.7.4.3',
            'mypy>=0.782',
            'mypy-extensions>=0.4.3',
            'data-science-types',
            'pytest',
            'pytest-pep8',
            'pytest-cov',
            'sqlalchemy-stubs>=0.3'
        ]
    },
    include_package_data=True,
)
