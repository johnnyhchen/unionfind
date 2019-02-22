#!/bin/bash
#SBATCH -p RM
#SBATCH -N 256
#SBATCH --ntasks-per-node 28
#SBATCH -t 04:00:00
#SBATCH -J kron-256-25-16

# Echo commands to stdout
set -x

# Move to kronecker directory
cd /home/jchoi157/unionfind/examples/kronecker

ppn=27
pes=$(( $SLURM_JOB_NUM_NODES * $ppn ))

./charmrun +p$pes ./graph-omp -s 25 -e 16 ++ppn $ppn
