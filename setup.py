from setuptools import setup
import re


setup(
    name="HeTu",
    author='Zhang Jianhao',
    author_email='heeroz@gmail.com',
    description='河图：基于ECS及DOD理念，整合数据库和逻辑的游戏服务器框架',
    long_description=open('README.md', encoding='utf-8').read(),
    license='Apache 2.0',
    keywords='online game server database ecs entity component system mmo multiplayer',
    url='https://github.com/Heerozh/HeTu',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11',

    version=re.findall(r"^__version__ = \"([^']+)\"\r?$",
                       open('hetu/__version__.py', encoding='utf-8').read(), re.M)[0],
    packages=['hetu', 'hetu.data', 'hetu.system', 'hetu.common',],
    entry_points={"console_scripts": "hetu=hetu.__main__:main"}
)
