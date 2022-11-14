#!/bin/bash
IP="$1"
TARGET_IP="$2"
ssh -i ~/.ssh/pelton pelton@$IP "echo \"export TARGET_IP=$TARGET_IP\" >> ~/.bashrc"
