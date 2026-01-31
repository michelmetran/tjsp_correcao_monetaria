from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

requirements = []
for line in open('requirements.txt'):
    li = line.strip()
    if not li.startswith('#'):
        requirements.append(line.rstrip())

VERSION = (0, 0, 7)  # (1, 0, 7, 'dev0')
__version__ = '.'.join(map(str, VERSION))

setup(
    # Nome (não precisa ser o nome do repositório, nem de qualquer pasta...)
    name='tjsp',
    version=__version__,
    author='Michel Metran',
    author_email='michelmetran@gmail.com',
    description='Série Temporal das Taxas do Tribunal de Justiça do Estado de São Paulo e métodos para recuperar informações das taxas',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/gaemapiracicaba/sp_tjsp_correcao_monetaria',
    keywords='python, tjsp, finance',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Natural Language :: Portuguese',
        'Intended Audience :: Developers',
    ],

    # Qual python? e packages?
    python_requires='>=3',
    install_requires=requirements,

    # Entry
    # Our packages live under src but src is not a package itself
    #package_dir={'': 'ufesp'},

    # Quando são diversos módulos...
    #packages=find_packages('ufesp', exclude=['test']),
    packages=['tjsp'],

    # Apenas um módulo...
    # py_modules=['decreto_estadual_8468'],  # Quando se trata apenas de um módulo

    # Dados
    include_package_data=True,
    package_data={'': ['data/tabela_debitos_judiciais.csv']},
)
