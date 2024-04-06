import os,time
import dask
from dask.distributed import Client,as_completed
import xarray

job_id=os.environ['SLURM_JOBID']
client=Client(scheduler_file='scheduler_%s.json'%job_id)
print(client)


datasets=xarray.open_mfdataset('%s/*.nc'%(os.environ['DATA_DIR']),
                               engine='netcdf4',
                               chunks={'time':50})


with dask.config.set(**{'array.slicing.split_large_chunks': False}):
    ytmp,dtmp=zip(*datasets.groupby('time.year'))

print(len(ytmp))
print(ytmp)



os.system('rm -rf %s/*'%(os.environ['RESULTS_DIR'])


paths=list()
for y in range(len(dtmp)):
    paths.append('%s/tmp_%d.nc'%(os.environ['RESULTS_DIR'],ytmp[y]))



def writer(path,D):
    D.load()
    var = list(D.variables)[-1]
    encoding = {
        'lat': {'zlib': False}, 
        'lon': {'zlib': False}, 
        var: {'missing_value': 1e+20, 
        '_FillValue': 1e+20, 
        'complevel': 1, 
        'zlib': False}}
    return D.to_netcdf(path=path,
    engine='netcdf4',
    encoding=encoding,
    format='NETCDF4_CLASSIC')




futures=list()
for i in range(len(dtmp)):
    futures.append(client.submit(writer,paths[i],dtmp[i]))

for future in as_completed(futures):
    print(future.status)





