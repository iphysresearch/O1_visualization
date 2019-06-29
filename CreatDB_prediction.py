#! usr/bin/python
# coding=utf-8


import sys,os,datetime
from tqdm import tqdm
import numpy as np
import pandas as pd
from itertools import product

from sqlalchemy import create_engine

address = os.path.join('../output/')
file_list = os.listdir(address)


disk_engine = create_engine('sqlite:///DataBase/O1_Prediction.db')

DQ_list = ['DATA', 'CBC_CAT1', 'CBC_CAT2', 'CBC_CAT3', 
           'BURST_CAT1', 'BURST_CAT2', 'BURST_CAT3', 
           'NO_CBC_HW_INJ', 'NO_BURST_HW_INJ', 'NO_DETCHAR_HW_INJ', 
           'NO_CW_HW_INJ', 'NO_STOCH_HW_INJ', 'DEFAULT']

index_start = 1
list_warning = []
for i, file in enumerate(tqdm(file_list)):
    # if file not in ['GPS1126989824_index_SNR0.02-fs4096-T5w1-27-frac5.output.npy',
                    # 'GPS1126993920_index_SNR0.02-fs4096-T5w1-27-frac5.output.npy',
                    # 'GPS1126985728_index_SNR0.02-fs4096-T5w1-27-frac5.output.npy']: # 172 
        # continue
    try:
        npy = np.load(os.path.join(address, file))
    except Exception as e:
        print(e)
        list_warning.append([i,file])
        np.save('list_warning', list_warning)
        print(i, file)
        continue

    
    df = pd.DataFrame({'Prediction': npy[0],
                       'GPS': npy[1], 
                       'ch': npy[2]}, )

    for i,j,k in product(['H1', 'L1'], DQ_list, range(5)):
        df.loc[:,'{}_{}_{}'.format(i,j,k)] = df.ch.map(lambda x: x[0 if i == 'H1' else 1][j][k] ) 
    df.drop(['ch'], axis=1, inplace=True)
    df.reset_index(inplace=True)    
    df.loc[:,'ID'] = df.index
    df.drop(['index'], axis=1, inplace=True)

    df.ID += index_start
    
    df.to_sql('data', disk_engine, if_exists='append')
    index_start = df.ID.iloc[-1] + 1

print(list_warning)

# 根据 GPS 对数据库中的 ID 去重
# http://docs.sqlalchemy.org/en/latest/core/connections.html#basic-usage
db_conn = disk_engine.connect()

db_conn.execute(
    """
    DELETE from data where ID not in (select ID from data Group by GPS)
    """
)
db_conn.execute(
    """
    VACUUM;
    """
)
db_conn.close()