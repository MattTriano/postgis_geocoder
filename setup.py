from setuptools import setup, find_packages

setup(
	author="Matt Triano",
    name="postgisgeocoder",
	description="A convenient postgis geocoder.",
    version="0.1.0",
	packages=find_packages(include=["postgisgeocoder", "postgisgeocoder.*"]),
    install_requires=["SQLAlchemy>=1.4", "psycopg2", "geopandas>=0.9", "tqdm"],
	include_package_data=True
)
