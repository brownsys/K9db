Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash
ORCHESTRATOR_IP="127.0.0.1"  # replace with actual IP for experiment.
BRANCH="master"

# install dependencies
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release

# install docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io

# Setup ssh keys to access pelton.
mkdir -p /home/ec2-user/.ssh

echo "-----BEGIN OPENSSH PRIVATE KEY-----" > /home/ec2-user/.ssh/id_rsa
echo "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn" >> /home/ec2-user/.ssh/id_rsa
echo "NhAAAAAwEAAQAAAQEAwff9pbuSAWAeZUrzSpBpKElI4Xv4BLvTYW2c/Sq49RLD6hslvjLK" >> /home/ec2-user/.ssh/id_rsa
echo "NhASC68xlQDfBfcpFH6eYVb4b8CJfKMYsxwA84HB6ks3Zevd+8q0pFJl5pbxOs2dK+Ddsl" >> /home/ec2-user/.ssh/id_rsa
echo "iiS9GUke8ki/zpAl5ACvYvYjzFWKi7xsYbniB+nIab8sN8FOdRxFXfv+4Dg9YeXDwYRXqH" >> /home/ec2-user/.ssh/id_rsa
echo "P1MA1WHami5suEpY7A/WaJFP0V7oIXa6Ht1cPOTwoiMRpT8gMAr0VI4cHKTZ6Zm7fhREMg" >> /home/ec2-user/.ssh/id_rsa
echo "1KGojImqGuLNt33aMtzJiQI7yFyIEl3uLNGMno9aN0X7uNs/MRljwnyb1i+t4T8BLzjbZJ" >> /home/ec2-user/.ssh/id_rsa
echo "zVyvzekM3wAAA9CXKMROlyjETgAAAAdzc2gtcnNhAAABAQDB9/2lu5IBYB5lSvNKkGkoSU" >> /home/ec2-user/.ssh/id_rsa
echo "jhe/gEu9NhbZz9Krj1EsPqGyW+Mso2EBILrzGVAN8F9ykUfp5hVvhvwIl8oxizHADzgcHq" >> /home/ec2-user/.ssh/id_rsa
echo "Szdl6937yrSkUmXmlvE6zZ0r4N2yWKJL0ZSR7ySL/OkCXkAK9i9iPMVYqLvGxhueIH6chp" >> /home/ec2-user/.ssh/id_rsa
echo "vyw3wU51HEVd+/7gOD1h5cPBhFeoc/UwDVYdqaLmy4SljsD9ZokU/RXughdroe3Vw85PCi" >> /home/ec2-user/.ssh/id_rsa
echo "IxGlPyAwCvRUjhwcpNnpmbt+FEQyDUoaiMiaoa4s23fdoy3MmJAjvIXIgSXe4s0Yyej1o3" >> /home/ec2-user/.ssh/id_rsa
echo "Rfu42z8xGWPCfJvWL63hPwEvONtknNXK/N6QzfAAAAAwEAAQAAAQBSvCay9IjYkVklt4yC" >> /home/ec2-user/.ssh/id_rsa
echo "t4pDJs8xbqFh04PcQNb/naN61WA/kQjOUfeyi7RGy5mIhvCaKMya8083EdOyRxvdz+uPKl" >> /home/ec2-user/.ssh/id_rsa
echo "i1hRljiGW+0ZbD+biHhqX0b1nBzUZHGwk1M102ndSTUr/x/hSdb/o9MrkPXACJSp+dr47E" >> /home/ec2-user/.ssh/id_rsa
echo "KfQXa8tFB2D7wNhss4jGbql6Gm5LLrnmZaTs1idgB/qHtjmjiBN7ig1itK706VCgRPySKo" >> /home/ec2-user/.ssh/id_rsa
echo "OuyYdQ/wwwuj5Xx70r69zBeMThuhXCwBuxN8NWm9OLGgQmfyDn88cXF4jcu9xluODPQkTq" >> /home/ec2-user/.ssh/id_rsa
echo "HRasywcy0rUrQ6JSrCepe328WxsPY7FevlCNCOJ64BMhAAAAgQDR7pAgyCwcrITnTD9xb6" >> /home/ec2-user/.ssh/id_rsa
echo "4MyGRc71MHV4yr8HtmPDwz+g+YVvcP/DE7dlvyTqzrYJvPSM88Ju+UxLdiJotJTm0YGpnV" >> /home/ec2-user/.ssh/id_rsa
echo "LXv3Unuc0ncANh0OtBZDmH7fnKowNdPW9yqSz3uLfVVsc3kP8ClOpd9nCMwu9Fex/Uz5aU" >> /home/ec2-user/.ssh/id_rsa
echo "KHVH6j/jSpIAAAAIEA8w5P1xWmUoNgikkDvGoV6dmiDvhHzWNOoi8yslt9c/N2W5X3815E" >> /home/ec2-user/.ssh/id_rsa
echo "nX+aySNbglFu7BoeTFh7fH37lFayU7ycBXLwMQgExjEY1xNCHYCweh7YFoAhEGGI4hTmKk" >> /home/ec2-user/.ssh/id_rsa
echo "z9t6/InnmHeChbOeI9z/5YeTwKfsGRsuDZYVC67xf2D+Tg3DcAAACBAMxMc59CDMnizo2O" >> /home/ec2-user/.ssh/id_rsa
echo "5b6ftLv/oCpicl2DgCbng1HpoKTmdg9X2BjZMlLq/Gh8Osgq85TshxfZMJViejNl/sYvWE" >> /home/ec2-user/.ssh/id_rsa
echo "5G3rV3ejunEYYr3x+Mpy2FdWZu7nE7UUhKjXX0cTwNkpCP6VLjWB88GOExxj7LNgWBSkKF" >> /home/ec2-user/.ssh/id_rsa
echo "wgRcKIJeRxuLThCZAAAAF2xlb25oYXJkc0BMZW9uaGFyZHMtTUJQAQID" >> /home/ec2-user/.ssh/id_rsa
echo "-----END OPENSSH PRIVATE KEY-----" >> /home/ec2-user/.ssh/id_rsa

echo "host github.com" > /home/ec2-user/.ssh/config
echo "     HostName github.com" >> /home/ec2-user/.ssh/config
echo "     IdentityFile /home/ec2-user/.ssh/id_rsa" >> /home/ec2-user/.ssh/config
echo "     StrictHostKeyChecking no" >> /home/ec2-user/.ssh/config
echo "     User git" >> /home/ec2-user/.ssh/config

# clone repo once!
if [[ ! -d "pelton" ]]
then
  git clone --recurse-submodules -j4 git@github.com:brownsys/pelton.git
fi

# update repo
cd pelton
git fetch origin
git checkout $BRANCH
git pull origin $BRANCH

# build docker image
docker build -t pelton/latest .
if [ $(docker ps -a | grep pelton-worker | wc -l) -ne 0 ]; then
  docker stop pelton-worker
  docker rm pelton-worker
fi

docker run --name pelton-worker -d -t pelton/latest
sleep 20

docker exec pelton-worker sh -c "cd /home/pelton/ && ./orchestrator/worker/daemon.sh $ORCHESTRATOR_IP"

--//
