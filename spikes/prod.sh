#!/bin/bash

export IFS=$'\n'
for i in `cat $1`; 
   do echo $i > myfifo;
   sleep 1;
done

