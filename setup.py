import codecs
from setuptools import setup, find_packages

VERSION = '0.0.0'

entry_points = {
    'console_scripts': [
        "nti_hypatia_indexer = nti.hypatia.utils.indexer:main",
    ],
}

setup(
    name='nti.hypatia',
    version=VERSION,
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="NTI search hypatia",
    long_description=codecs.open('README.rst', encoding='utf-8').read(),
    license='Proprietary',
    keywords='pyramid preference',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
		'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        ],
	packages=find_packages('src'),
	package_dir={'': 'src'},
	namespace_packages=['nti'],
	install_requires=[
		'setuptools',
        'hypatia',
        'zc.catalogqueue',
        'zopyx.txng3.ext'
	],
	entry_points=entry_points
)
