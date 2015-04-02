from setuptools import setup

setup (
    name = "RabbitToRaven",
    version = "0.1",
    description="RabbitToRaven is a utility for copying RabbitMQ messages into a RavenDB database.",
    author="Greg Major",
    author_email="", # Removed to limit spam harvesting.
    url="http://www.gregmajor.com/",
    packages = find_packages(exclude="test"),
    entry_points = {
        'console_scripts': ['RabbitToRaven = RabbitToRaven.__main__:main']
                    },
    download_url = "http://www.gregmajor.com/",
    zip_safe = True
)
