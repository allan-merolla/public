df.write.mode("overwrite").saveAsTable( \
                                             'tblAccount', \
                                             mergeSchema='true', \
                                             format='delta', \
                                             mode='overwrite', \
                                             path='/source/system/account.delta' \
                                             )
