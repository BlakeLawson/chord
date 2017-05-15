#!/bin/bash
I=0
while [ $I -lt 10 ]; do
  mkdir serv$I
  let I=I+1
done
