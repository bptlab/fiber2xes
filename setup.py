from setuptools import setup, find_packages  # type: ignore


requirements = [
    'sqlalchemy~=1.3.3',
    'sqlalchemy-hana~=0.3.0',
    'opyenxes~=0.3.0',
    'pandas~=1.3.0',
    'pyspark~=3.1.2',
    'python-dotenv~=0.19.0'
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
            'typing-extensions~=3.10.0.0',
            'mypy~=0.910',
            'mypy-extensions~=0.4.3',
            'data-science-types',
            'pytest~=6.2.4',
            'pytest-pep8~=1.0.6',
            'pytest-cov~=2.12.1',
            'pytest-env~=0.6.2',
            'pylint~=2.9.6',
            'sqlalchemy-stubs~=0.4',
            'chispa~=0.8.2'
        ]
    },
    include_package_data=True,
)
