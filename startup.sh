#!/bin/bash
set -e

mkdir -p ./dags ./logs
chmod -R +x script

sudo chmod -R 777 dags
sudo chmod -R 777 logs

sudo docker compose up

