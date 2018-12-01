#!/bin/bash -xe
sudo yum -y install git emacs tmux
cd /home/hadoop
git clone https://github.com/seandavi/omicidx.git
chown -R hadoop omicidx
cd omicidx
sudo pip-3.4 install elasticsearch
sudo pip-3.4 install elasticsearch_dsl
sudo pip-3.4 install requests
sudo pip-3.4 install jupyterlab
sudo pip-3.4 install numpy
sudo pip-3.4 install sqlalchemy bokeh pandas ggplot plotly
sudo pip-3.4 install psycopg2
sudo pip-3.4 install findspark
sudo pip-3.4 install jedi


