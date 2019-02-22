#!/bin/bash
#SBATCH -J kron-2-s27-e16
#SBATCH -oe kron.%j
#SBATCH -p normal
#SBATCH -N 2
#SBATCH -n 8
#SBATCH -t 04:00:00

ulimit -c unlimited
date
ibrun /home1/04332/jchoi157/unionfind/examples/kronecker/graph-omp -s 27 -e 16 ++ppn 16 +pemap 0-63 +commap 64-67 +LBOff
date
