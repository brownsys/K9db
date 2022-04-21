#!/bin/bash
mariadb -u pelton -ppassword -e "DROP DATABASE IF EXISTS lobsters";
mariadb -u pelton -ppassword -e "CREATE DATABASE lobsters";
mariadb -u pelton -ppassword lobsters < lobsters.sql
