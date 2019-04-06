#!/bin/bash

node_id=$1
python /usr/local/presto/scripts/render.py --node-id $node_id etc/node.properties.template
/usr/local/presto/bin/launcher run




