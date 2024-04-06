# Introduction

This template is to read NetCDF files using xarray package as Dask datasets do analytics on it. 

# Testing
The jobscript ```shaheen3.slurm``` can be configure to point to the input data and and a results directory to write output file.

Change the ```--memory-limit``` paramerter in ```shaheen3.slurm``` file to 376/${SLURM_NTASKS_PER_NODE} GiB``` when changing the ```#SBATCH --ntasks-per-node``` directive
