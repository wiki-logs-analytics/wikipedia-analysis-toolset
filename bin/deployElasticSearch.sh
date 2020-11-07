#!/bin/bash

sudo docker pull elasticsearch:7.9.3
sudo docker pull docker.elastic.co/kibana/kibana:7.9.3
sudo docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "http.cors.enabled=true" -e "http.cors.allow-origin=http://localhost:8080" -e "discovery.type=single-node" elasticsearch:7.9.3
sudo docker run -d --link  elasticsearch:elasticsearch  -p 5601:5601   docker.elastic.co/kibana/kibana:7.9.3 