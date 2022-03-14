# Go to home directory.
cd ~

# Make sure ssh deploy key is added.
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/nsdi

# Install docker.
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io

# Add user to docker group.
sudo gpasswd -a $USER docker

# Update group membership.
newgrp docker


# Do this on server: https://cloud.google.com/architecture/mysql-remote-access
