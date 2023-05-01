#!/bin/bash

# Go to plotting directory
cd experiments/scripts/plotting

# Install latex dependencies
sudo apt-get install -y texlive-latex-base texlive-fonts-recommended texlive-fonts-extra dvipng cm-super

# Install python3 virtualenv.
sudo apt-get install -y python3.8-venv

# Create virtual env and install requirements
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt || /bin/true
deactivate
