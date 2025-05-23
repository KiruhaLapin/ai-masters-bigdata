#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fields

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("1a.joblib")


#read and infere
read_opts=dict(
        sep='\t', names=fields[0:1]+fields[2:], index_col=False, header=None,
        iterator=True, chunksize=100
)


for df in pd.read_csv(sys.stdin, **read_opts):
    pred = model.predict_proba(df)
    out = zip(df.id.values, pred)
    print("\n".join([f"{i[0]}\t{i[1][1]}" for i in out]))

