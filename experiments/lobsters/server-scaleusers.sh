#!/bin/bash
cd /home/pelton/pelton

scales=("0.863" "1.73" "2.59" "3.45")
users=("5000" "10000" "15000" "20000")

echo "pelton ...."
sudo service mariadb stop
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  user=${users[$index]}
  echo "pelton-$user"
  ./run.sh opt --hostname="${LOCAL_IP}:10001" > log-${user}.txt 2>&1
done

echo "baseline ..."
sudo service mariadb start
