import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/kachery-daemon",
        "bin/kachery-daemon-start",
        "bin/kachery-daemon-info"
    ],
    install_requires=[
        "click",
        "simplejson",
        "requests",
        "jinjaroot"
    ]
)
