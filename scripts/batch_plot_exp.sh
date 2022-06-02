#!/usr/bin/env bash

base_folder=$1
output_folder=$2

if [ "$#" -lt 2 ]; then
  echo "Illegal number of parameters"
  echo "$0 <base_folder> <output_folder>"
  exit 1
fi

mkdir -p $output_folder
for d in $base_folder/*; do
  last_path=${d##*/}
  params=(${last_path//_/ })

  if [ "${#params[@]}" != 5 ];
  then
    echo "Skipping file $d"
    continue
  fi

  rf="${params[0]}"
  pa="${params[1]}"
  co="${params[2]}"
  po="${params[3]}"
  vr="${params[4]}"

  echo "Params: rf ${rf}, pa ${pa}, co ${co}, po ${po}, vr ${vr}"

  # for param in "${params[@]}"
  # do
  #   echo "${param}"
  # done

  cp graph3.py temp_graph.py

  # sed "s/^vrs[[:space:]]*=[[:space:]]*\[22\]/vrs = \[$vr\]/g" temp.py
  # sed "s/^vrs[[:space:]]*=[[:space:]]*\[22\]/vrs = \[$vr\]/g" temp.py
  # sed "s/vrs = \[22\]/abc/g" temp.py
  # sed "s/rfs = [3]/rfs = [$rf]/g; s/pas = [4]/pas = [$pa]/g; s/cos = [4]/ cos = [$co]/g; s/pos = [4]/pos = [$po]/g; s/vrs = [22]/vrs = [$vr]/g;" graph3.py > temp_graph.py
  sed -i "" -e "s/rfs[[:space:]]*=[[:space:]]*\[3\]/rfs = \[$rf\]/" temp_graph.py
  sed -i "" -e "s/pas[[:space:]]*=[[:space:]]*\[4\]/pas = \[$pa\]/" temp_graph.py
  sed -i "" -e "s/cos[[:space:]]*=[[:space:]]*\[4\]/cos = \[$co\]/" temp_graph.py
  sed -i "" -e "s/pos[[:space:]]*=[[:space:]]*\[4\]/pos = \[$po\]/" temp_graph.py
  sed -i "" -e "s/vrs[[:space:]]*=[[:space:]]*\[22\]/vrs = \[$vr\]/" temp_graph.py

  python3 temp_graph.py $d $output_folder ""
  echo

  # sleep 5

  rm temp_graph.py
  # break
done

