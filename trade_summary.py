#!/usr/bin/python
import sys
import math
import logging
import argparse

import pandas as pd
import numpy as np
import contextlib

"""
    Usage: python3 trade_summary.py trades.csv summary.csv
"""

def get_stats(x):
    results=dict(  avgBuy=(
                              ( x.buy_quantity * x.buy_price ).sum() / x.buy_quantity.sum()
                                if x.buy_quantity.sum() != 0
                                else 0 
                           ),
                   
                   avgSell=(
                                ( x.sell_quantity * x.sell_price).sum() / x.sell_quantity.sum()
                                if x.sell_quantity.sum() != 0
                                else 0 
                            ),
                 
                   nbTrades=(
                                int((x.price).count())
                            )
                )
    
    logging.debug(results)
    return pd.Series(results)


def read_data(file_path):
    """ Redirect Error lines to log """
    logging.info("Reading Trades data")
    try:
        #trades = pd.read_csv(file_path, index_col=False, encoding='utf-8', 
        #                                warn_bad_lines=True, error_bad_lines=False)
        trades = pd.read_csv(file_path, encoding='utf-8', warn_bad_lines=True, error_bad_lines=False)
        logging.info("No. of records read {}".format(trades.shape[0]))
    except Exception as e:
        logging.error("Error Occurred when reading trade file. {}".format(e))

    #trades = pd.read_csv(file_path, encoding='utf-8', warn_bad_lines=True, error_bad_lines=False)
    #logging.info("No. of records read {}".format(trades.shape[0]))
    logging.debug(trades.head()) 
    return trades

def data_processing(trades):
    """ Data Processing """
    logging.info("Processing Trades data")
    trades['buy_price']=trades.apply(lambda x :  x.price if x.quantity >= 0 else 0, axis=1)
    trades['sell_price']=trades.apply(lambda x :  x.price if x.quantity < 0 else 0, axis=1)
    trades['buy_quantity']=trades.apply(lambda x :  x.quantity if x.quantity >= 0 else 0, axis=1)
    trades['sell_quantity']=trades.apply(lambda x :  x.quantity if x.quantity < 0 else 0, axis=1)
    
    logging.debug(trades)
    return trades
    
def get_summary(trades):
    """ Get Trade Summary """ 
    logging.info("Getting Trade Summary")
    sum_trade=trades.groupby('product').apply(lambda x: get_stats(x))
    logging.debug(sum_trade)
    return sum_trade

def save_result(sum_trade,output):
    sum_trade.to_csv(output)
    logging.info("\n"+sum_trade.__repr__())
    logging.info("summary completed. Saved to {}".format(output))
   
if __name__ == "__main__" :
    

    # set up logging to file
    logging.basicConfig(
         filename='trade_summary.log',
         level=logging.INFO, 
         format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
         datefmt='%H:%M:%S'
     )
    
    
    # Read input argument
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath', help='enter path to trade csv file')
    parser.add_argument('output', help='enter path to summary csv file')
    args = parser.parse_args()

        
    #print(vars(args))
    if args.filepath == None:
        parser.print_help()
        raise Exception("Filepath argument is None")
    if args.output == None:
        parser.print_help()
        raise Exception("Output path argument is None")
    
    df_raw=read_data(args.filepath)
    
    df_trades=data_processing(df_raw)
    
    df_summary=get_summary(df_trades)
    
    save_result(df_summary,args.output)
    print(df_summary.head())
    print("Completed")