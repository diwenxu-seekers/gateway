from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="gateway_lib",
    version="1.2.2.1",
    author="WRC",
    author_email="hoffman.tsui@waveridercapital.com",
    description="A gateway to connect all brokers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://git/core/gateway",
    packages=find_packages('./src'),
    package_dir={
        'gateway_lib': 'src/gateway_lib'
    },
    platforms=['any'],
    license='Commercial License',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'ibapi',
        'pytz',
        'protobuf',
        'websockets'
    ]
)
