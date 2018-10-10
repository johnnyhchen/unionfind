#!/bin/bash

#PBS -N unionFind
#PBS -l nodes=135:ppn=32
#PBS -l walltime=02:00:00

CHARES_PER_CORE=4
CORES_PER_NODE=30
MESHPIECE_SIZE=64
#prob=0.2

meshpiece_count=$(expr $MESHPIECE_SIZE \* $MESHPIECE_SIZE)

logfile="union_find_reserve.results"

for prob in 0.4 0.6 0.8; do
#for MESHPIECE_SIZE in 64; do
  for dim in 8192; do
    size=$(expr $dim \* $dim)
    meshpiece_count=$(expr $MESHPIECE_SIZE \* $MESHPIECE_SIZE)
    total_cores=$(expr $size / $meshpiece_count / $CHARES_PER_CORE)
    total_cores=$(($total_cores > 1 ? $total_cores : 1))
    total_cores=$(($total_cores < $CORES_PER_NODE ? $CORES_PER_NODE : $total_cores))
    total_nodes=$(expr $total_cores + $CORES_PER_NODE - 1)
    total_nodes=$(expr $total_nodes / $CORES_PER_NODE \* 2)
    aprun -n 270 -N 2 /u/sciteam/sheth/unionFindFP/examples/prob_mesh/mesh $dim $MESHPIECE_SIZE $prob ++ppn 15 +pemap 1-15,17-31 +commap 0,1 > out

    tree_time=`grep -i "Tree construction time" out | cut -d ":" -f 2 | tr -d " "`
    component_time=`grep -i "Components detection time" out | cut -d ":" -f 2 | tr -d " "`
    total_time=`grep -i "Final runtime" out | cut -d ":" -f 2 | tr -d " "`
    echo "$dim $prob $tree_time $component_time $total_time" >> $logfile
    #exit #comment out this line after testing script on interactive node
  done
done
