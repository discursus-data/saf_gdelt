from setuptools import find_packages, setup

setup(
    name='discursus_gdelt',
    packages=find_packages(include=['discursus_gdelt']),
    scripts=[
        'discursus_gdelt/miners/gdelt_events_miner.zsh', 
        'discursus_gdelt/miners/gdelt_mentions_miner.zsh'
    ],
    version='0.1.0',
    description='GDELT mining package for the discursus platform',
    author='Olivier Dupuis',
    license='MIT',
)