import pandas as pd
import time
from mpi4py import MPI
import omdb

API_KEYS = ['602a4544', '9f67b61b', '810d1060', 'e21ca34a', '8e12167e']
file = "ids.csv"
ids = list(pd.read_csv(file)['IDS'])
data_list = [None] * len(ids)

def get_data():
    # Get rank of process and overall size of communicator:
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    API_KEY = API_KEYS[rank]
    omdb.set_default('apikey', API_KEY)
    interval = int(len(ids)/size)
    start = rank * interval
    end = (rank + 1) * interval

    for i in range(start, end):
        print(i)
        data_list[i] = omdb.imdbid(ids[i], fullplot=True, tomatoes=True)

def main():
    get_data()

if __name__ == '__main__':
    t0 = time.time()
    main()
    print('time is ', time.time()-t0)
