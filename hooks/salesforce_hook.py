from airflow.hooks.base_hook import BaseHook
from simple_salesforce import Salesforce

class SalesforceHook(BaseHook):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs
    ):
        """
        Borrowed from airflow.contrib

        Create new connection to Salesforce
        and allows you to pull data out of SFDC and save it to a file.
        You can then use that file with other
        Airflow operators to move the data into another data source
        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Salesforce.
                            The conenction shoud be type `http` and include a
                            user's security token in the `Extras` field.

        .. note::
            For the HTTP connection type, you can include a
            JSON structure in the `Extras` field.
            We need a user's security token to connect to Salesforce.
            So we define it in the `Extras` field as:
                `{"security_token":"YOUR_SECRUITY_TOKEN"}`
        """

        self.sf = None
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def get_conn(self):
        """
        Sign into Salesforce.
        If we have already signed it, this will just return the original object
        """
        if self.sf:
            return self.sf

        auth_type = self.extras.get('auth_type', 'password')

        if auth_type == 'direct':
            auth_kwargs = {
                'instance_url': self.connection.host,
                'session_id': self.connection.password
            }

        else:
            auth_kwargs = {
                'username': self.connection.login,
                'password': self.connection.password,
                'security_token': self.extras.get('security_token'),
                'instance_url': self.connection.host
            }
        # connect to Salesforce
        self.sf = Salesforce(**auth_kwargs)

        return self.sf