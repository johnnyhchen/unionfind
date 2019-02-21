#!/bin/bash
#SBATCH -N 1
#SBATCH -p RM
#SBATCH --ntasks-per-node 28
#SBATCH -t 8:00:00

# Echo commands to stdout
set -x

cd /home/jchoi157/unionfind/examples/kronecker
export OMP_NUM_THREADS=28
./graph-omp -s 27 -e 16 -g -o /pylon5/ac7k4vp/jchoi157/kronecker/s27-e16.dat
