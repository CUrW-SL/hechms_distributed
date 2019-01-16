from setuptools import setup

setup(
    name='hechms-distributed',
    version='1.0.0',
    packages=['input', 'input.run', 'input.gage', 'input.control', 'input.rainfall', 'input.shape_util', 'model', 'output', 'template', 'resources'],
    url='http://www.curwsl.org/',
    license='',
    author='hasitha',
    author_email='hasithadkr7@gmail.com',
    description='',
    install_requires=['FLASK', 'Flask-Uploads', 'Flask-JSON', 'pandas','numpy'],
    zip_safe=True
)
