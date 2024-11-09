import pandas as pd
from sodapy import Socrata
from typing import Generator
from dotenv import load_dotenv
import os

from data.constants import *

class CTAClient(Socrata):
    """
    Exposes CTA-specific transformations on data accessed through Socrata API.
    Implemented as a subclass of Socrata so we don't have multiple
    clients clogging up the global environment.
    """
    def __init__(self, timeout:int):
        load_dotenv()
        app_token = os.environ.get('SOCRATA_APP_TOKEN')
        super().__init__("data.cityofchicago.org", app_token=app_token, timeout=timeout)

    
    def soda_get_all(self, resource, has_header=True, **params):
        """Wrapper for client.get_all"""
        if "limit" in params.keys():
            # Sodapy doesn't allow limit with get_all :(
            data = iter(self.get(resource, **params))
        else:
            data = self.get_all(resource, **params)
        return (self._soda_to_df(data, has_header)
                .pipe(self._soda_fix_coltypes, resource, params.get('select',None)))


    def _soda_to_df(self, data: Generator, has_header=True):
        """
        Collect iterable of rows into a dataframe.
        """
        if has_header:
            header = list(next(data, {}).keys())
            df = pd.DataFrame.from_records(data, columns=header)
        else:
            df = pd.DataFrame.from_records(data)
        return df


    def soda_get_coltypes(self, resource):
        """Query basic table metadata"""
        meta = self.get_metadata(resource)
        colnames = [c['fieldName'] for c in meta['columns']]
        coltypes = [c['dataTypeName'] for c in meta['columns']]
        coltypes = {c: ct for c,ct in zip(colnames, coltypes)}
        return coltypes
        

    def _soda_fix_coltypes(self, df: pd.DataFrame, resource, select:str=None):
        """
        Coerce pandas dtypes because SodaPy seems to return everything as strings.
        """
        # Iterate through columns because query may subset columns
        coltypes = self.soda_get_coltypes(resource)
        if select is not None:
            for kv in select.split(','):
                if ' AS ' in kv:
                    k = kv.split(' AS ')[0].strip()
                    v = kv.split(' AS ')[1].strip()
                    # Imperfectly extract column name from univariate expressions
                    # XXX: Assumes the function doesn't change the dtype
                    #      If we dont want that assumption we should only alias
                    #      if k exactly equals a column name ie no function
                    c = next(filter(lambda c: c in k, coltypes.keys()), None)
                    if c is not None:
                        coltypes[v] = coltypes[c]

        for col in df.columns:
            if col not in coltypes.keys():
                continue # functions of columns might not preserve type!
            elif coltypes[col] == 'calendar_date':
                df[col] = pd.to_datetime(df[col])
            elif coltypes[col] == 'number':
                df[col] = pd.to_numeric(df[col])
        return df
