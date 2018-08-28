#!/bin/bash -xe
sudo yum -y install git emacs tmux
cd /home/hadoop
git clone https://github.com/seandavi/omicidx.git
chown -R hadoop omicidx
cd omicidx
sudo pip-3.4 install .
sudo pip-3.4 install elasticsearch
sudo pip-3.4 install elasticsearch_dsl
sudo pip-3.4 install requests
sudo pip-3.4 install jupyterlab
sudo pip-3.4 install sqlalchemy bokeh pandas ggplot plotly
sudo pip-3.4 install psycopg2
sudo pip-3.4 install findspark
sudo pip-3.4 install jedi
sudo pip-3.4 install pyspark

sudo echo '' >> /home/hadoop/.bash_profile
sudo echo 'export SPARK_HOME=/usr/lib/spark' >> /home/hadoop/.bash_profile
sudo cat <<EOF > /home/hadoop/.bash_profile
# set the default region for the AWS CLI
export JAVA_HOME=/etc/alternatives/jre

export SPARK_HOME=/usr/lib/spark
export PYSPARK_PYTHON=python34
export PYSPARK_DRIVER_PYTHON=python34
EOF

