import json
import traceback
from enum import Enum
from logging import DEBUG, StreamHandler, getLogger

import pandas as pd
import requests
from apiclient.discovery import build
from google.cloud import datastore

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False


class PandasWrapper(object):

  __instance = None

  def __new__(cls, *args, **keys):
    if cls.__instance is None:
      cls.__instance = object.__new__(cls)
    return cls.__instance

  def __init__(self, project_id):
    self.datastore_client = datastore.Client(project_id)

  def dataframe_to_datastore(self, df, datastore_kind_name=None, update_keys=None, if_exists="skip"):

    logger.debug("datastore_kind_name:" + str(datastore_kind_name))
    logger.debug("update_keys:" + str(update_keys))
    if datastore_kind_name is None or datastore_kind_name == "":
      return

    count = 0
    for index, row in df.iterrows():
      result = None
      query = self.datastore_client.query(kind=datastore_kind_name)
      if update_keys is not None and isinstance(update_keys, list):
        for k in update_keys:
          query.add_filter(k, '=', row[k])

        query_iter = query.fetch()
        result = list(query_iter)

      task = datastore.Entity(self.datastore_client.key(datastore_kind_name))

      if result is not None and len(result) > 0:
        if if_exists == "skip":
          logger.debug("skip:" + str(result))
          continue
        elif if_exists == "replace":
          logger.debug("replace:" + str(result))
          task = result[0]
          logger.debug("update:" + str(result))
        elif if_exists == "append":
          logger.debug("insert:" + str(result))
        else:
          logger.debug("args error.")
          return
      else:
        logger.debug("insert:" + str(result))

      for col_name in df.columns:
        val = None
        if row[col_name] is not None:
          if isinstance(row[col_name], str):
            val = row[col_name][:1500]
          else:
            val = row[col_name]
        task[col_name] = val

      logger.debug("task:" + str(task))
      self.datastore_client.put(task)
      count += 1

    logger.debug("proc done." + "count:" + str(count))

  def set_diff_dataframes(self, df_, df2_, join_keys=None):
    i1 = df_.set_index(join_keys).index
    i2 = df2_.set_index(join_keys).index
    df = df_[~i1.isin(i2)]
    return df
