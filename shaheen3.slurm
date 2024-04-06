#!/bin/bash -l 
#SBATCH --time=01:0:0
#SBATCH --ntasks=32
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=48
#SBATCH --hint=nomultithread
#SBATCH --mem=376G

module swap PrgEnv-cray PrgEnv-gnu
module load python

export LC_ALL=C.UTF-8
export LANG=C.UTF-8



# setup ssh tunneling
# get tunneling info
node=$(hostname -s)
user=$(whoami)
submit_host=${SLURM_SUBMIT_HOST}


export DATA_DIR=/scratch/shaima0d/dask/user_codes/big_data_1
export RESULTS_DIR=/scratch/shaima0d/dask/results/
mkdir -p $RESULTS_DIR



srun -c $SLURM_CPUS_PER_TASK -n $SLURM_NTASKS -N ${SLURM_NNODES} \
--cpu-bind=cores --hint=nomultithread \
dask-mpi  --nthreads ${SLURM_CPUS_PER_TASK} \
		--memory-limit="94GiB" \
		--local-directory=${PWD}/workers${SLURM_JOBID} \
		--scheduler-file=scheduler_${SLURM_JOBID}.json --interface=hsn0 \
		--scheduler-port=10000 --dashboard-address=10001 \
		--worker-class distributed.Worker &

echo "
To connect to the Dask Dashboard, copy the following line and paste in new termial, then using URL in a browser : localhost:10001 

ssh -L 10001:${node}:10001 ${user}@${submit_host}.hpc.kaust.edu.sa
"


sleep 10
time -p  python dask_futures_xarray.py

