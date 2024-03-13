#!/bin/bash
variable="/portal,/data,/org"
for server in ${variable//,/ }
do
  echo -----------------------$server-------------------------
done


