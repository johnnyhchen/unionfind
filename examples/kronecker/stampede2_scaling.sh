#!/bin/bash
#SBATCH -p normal
#SBATCH -N 16
#SBATCH -n 64
#SBATCH -t 01:00:00
#SBATCH -J kron-16-s25-e16-mpi
#SBATCH -o %x.stdout
#SBATCH -e %x.stderr

ulimit -c unlimited
date
ibrun /home1/04332/jchoi157/unionfind/examples/kronecker/graph-omp -s 25 -e 16 ++ppn 16 +pemap 0-63 +commap 64-67 +LBOff
date
