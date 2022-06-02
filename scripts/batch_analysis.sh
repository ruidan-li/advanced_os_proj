#!/usr/bin/env bash

base_folder=$1
output_folder=$2

if [ "$#" -lt 2 ]; then
  echo "Illegal number of parameters"
  echo "$0 <base_folder> <output_folder>"
  exit 1
fi

for d in $base_folder/*; do
  last_path=${d##*/}
  python3 analysis3.py $last_path "$output_folder/analysis" $d
  echo
done
