from airflow.contrib.hooks.salesforce_hook import SalesforceHook
import copy
from simple_salesforce import Salesforce
import pandas as pd
import time


class SalesforceYieldingHook(SalesforceHook):

    def __init__(self, conn_id, *args, **kwargs):
        super(SalesforceYieldingHook, self).__init__(conn_id, *args, **kwargs)

    def yield_all(self, query, include_deleted=False, **kwargs):
        """yield paged results and schema for the `query`. This is a convenience
        wrapper around `query(...)` and `query_more(...)`.
        * query -- the SOQL query to send to Salesforce, e.g.
                   SELECT Id FROM Lead WHERE Email = "waldo@somewhere.com"
        * include_deleted -- True if the query should include deleted records.
        """

        self.log.info("Querying for all objects")
        result = self.sf.query(query,
                               include_deleted=include_deleted,
                               **kwargs)
        if len(result['records']) == 0:
            return None
        object_name = result['records'][0]['attributes']['type']
        schema = self.describe_object(object_name)
        all_records = []

        while True:
            yield (result, schema)
            # fetch next batch if we're not done else break out of loop
            if not result['done']:
                result = self.sf.query_more(
                    result['nextRecordsUrl'],
                    identifier_is_url=True,
                    include_deleted=include_deleted)
            else:
                break
        return None

    def format_results(
        self,
        query_results,
        schema,
        fmt="csv",
        coerce_to_timestamp=False,
        record_time_added=False
    ):
        """
        copied from write_object_to_file which never appends , instead of writing to file format_results return the results as string in the desired format
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json', 'ndjson']:
            raise ValueError("Format value is not recognized: {0}".format(fmt))

        # this line right here will convert all integers to floats if there are
        # any None/np.nan values in the column
        # that's because None/np.nan cannot exist in an integer column
        # we should write all of our timestamps as FLOATS in our final schema
        df = pd.DataFrame.from_records(query_results, exclude=["attributes"])

        df.columns = [c.lower() for c in df.columns]

        # convert columns with datetime strings to datetimes
        # not all strings will be datetimes, so we ignore any errors that occur
        # we get the object's definition at this point and only consider
        # features that are DATE or DATETIME
        if coerce_to_timestamp and df.shape[0] > 0:
            # get the object name out of the query results
            # it's stored in the "attributes" dictionary
            # for each returned record
            object_name = query_results[0]['attributes']['type']

            self.log.info("Coercing timestamps for: %s", object_name)

            # possible columns that can be convereted to timestamps
            # are the ones that are either date or datetime types
            # strings are too general and we risk unintentional conversion
            possible_timestamp_cols = [
                i['name'].lower()
                for i in schema['fields']
                if i['type'] in ["date", "datetime"] and
                i['name'].lower() in df.columns
            ]
            df[possible_timestamp_cols] = df[possible_timestamp_cols].apply(
                lambda x: self._to_timestamp(x)
            )

        if record_time_added:
            fetched_time = time.time()
            df["time_fetched_from_salesforce"] = fetched_time

        # write the CSV or JSON file depending on the option
        # NOTE:
        #   datetimes here are an issue.
        #   There is no good way to manage the difference
        #   for to_json, the options are an epoch or a ISO string
        #   but for to_csv, it will be a string output by datetime
        #   For JSON we decided to output the epoch timestamp in seconds
        #   (as is fairly standard for JavaScript)
        #   And for csv, we do a string
        if fmt == "csv":
            # there are also a ton of newline objects
            # that mess up our ability to write to csv
            # we remove these newlines so that the output is a valid CSV format
            self.log.info("Cleaning data and writing to CSV")
            possible_strings = df.columns[df.dtypes == "object"]
            df[possible_strings] = df[possible_strings].apply(
                lambda x: x.str.replace("\r\n", "")
            )
            df[possible_strings] = df[possible_strings].apply(
                lambda x: x.str.replace("\n", "")
            )

            # write the dataframe
            return df.to_csv(index=False)
        elif fmt == "json":
            return df.to_json(orient="records", date_unit="s")
        elif fmt == "ndjson":
            return df.to_json(orient="records", lines=True, date_unit="s")
