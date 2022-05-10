from setuptools import setup

setup(
    name='worker',
    version='0.6.2',
    description='Tracardi Customer Data Platform Import worker',
    author='Risto Kowaczewski',
    author_email='risto.kowaczewski@gmail.com',
    packages=['worker'],
    install_requires=[
        'celery == 5.2.6',
        'redis'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=['tracardi'],
    include_package_data=True,
    python_requires=">=3.8",
)
