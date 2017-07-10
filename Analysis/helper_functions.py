import pymysql as sql
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from sklearn import preprocessing
import math
import os
import rds_config
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.formula.api as smf


def standard_scaler(df, cols=None):
    if cols is None:
        cols = list(df)
        std_scale = preprocessing.StandardScaler().fit(df)
        return pd.DataFrame(std_scale.transform(df), columns=cols)
    else:
        std_scale = preprocessing.StandardScaler().fit(df[cols])
        std_df = pd.DataFrame(std_scale.transform(df[cols]), columns=cols)
        df.drop(cols, axis=1, inplace=True)
        return pd.concat([df, std_df], axis=1)


def min_max_scaler(df, cols=None):
    if cols is None:
        cols = list(df)
        std_scale = preprocessing.StandardScaler().fit(df)
        return pd.DataFrame(std_scale.transform(df), columns=cols)
    else:
        minmax_scale = preprocessing.MinMaxScaler().fit(df[cols])
        minmax_df = pd.DataFrame(minmax_scale.transform(df[cols]), columns=cols)
        df.drop(cols, axis=1, inplace=True)
        return pd.concat([df, minmax_df], axis=1)


def create_db_engine():
    sql.install_as_MySQLdb()

    rds_host = rds_config.db_endpoint
    name = rds_config.db_username
    password = rds_config.db_password
    db_name = rds_config.db_name

    conn_str = "mysql+pymysql://{nm}:{pswrd}@{host}:3306/{dbnm}" \
        .format(nm=name, pswrd=password, host=rds_host, dbnm=db_name)

    return create_engine(conn_str)


def create_expanding_mean_col(df, col_name, new_col_name, groupby_col, min_periods=1):
    df[new_col_name] = df[col_name].groupby(df[groupby_col]).apply(pd.expanding_mean, min_periods=min_periods)
    df[new_col_name] = df[new_col_name].groupby(df[groupby_col]).shift(1)

    return df


def create_expanding_sum_col(df, col_name, new_col_name, groupby_col):
    df[new_col_name] = df[col_name].groupby(df[groupby_col]).apply(pd.expanding_sum)
    df[new_col_name] = df[new_col_name].groupby(df[groupby_col]).shift(1)

    return df


def minutes_played(row):
    return float(row.split(':')[0]) + (float(row.split(':')[1]) / 60)