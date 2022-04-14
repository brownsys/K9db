#!/bin/bash
mariadb -u root -ppassword -e "DROP DATABASE IF EXISTS lobsters";
mariadb -u root -ppassword -e "CREATE DATABASE lobsters";
mariadb -u root -ppassword lobsters < lobsters.sql
