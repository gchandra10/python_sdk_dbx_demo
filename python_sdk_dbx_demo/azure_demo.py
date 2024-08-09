import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import iam

# Load environment variables from .env file
load_dotenv()


class UserService:
    """
    A service class to interact with Databricks user data.

    Attributes:
        client: A client object to interact with the Databricks API.
    """

    def __init__(self, client):
        """
        Initializes the UserService with a given client.

        Args:
            client: An instance of a Databricks client (WorkspaceClient or AccountClient).
        """
        self.client = client

    def list_users(
        self,
        attributes: str,
        sort_by: str,
        user_filter: str,
        sort_order: iam.ListSortOrder,
    ):
        """
        Lists users from the Databricks workspace or account based on provided criteria.

        Args:
            attributes: The user attributes to retrieve (e.g., "id,userName").
            sort_by: The attribute to sort by (e.g., "userName").
            user_filter: The filter to apply to the user listing (e.g., "userName co gc").
            sort_order: The order in which to sort the results (ascending or descending).

        Returns:
            A list of users matching the specified criteria.
        """
        return self.client.users.list(
            attributes=attributes,
            sort_by=sort_by,
            filter=user_filter,
            sort_order=sort_order,
        )

    def get_filtered_users(self, user_filter: str):
        """
        Retrieves and returns a filtered list of users.

        Args:
            user_filter: The filter to apply to the user listing (e.g., "userName co gc").

        Returns:
            A list of dictionaries, each containing user id and user name.
        """
        all_users = self.list_users(
            attributes="id,userName",
            sort_by="userName",
            user_filter=user_filter,
            sort_order=iam.ListSortOrder.DESCENDING,
        )
        lst_user = []
        if len(lst_user) == 0:
            for u in all_users:
                user_dict = {"id": u.id, "user_name": u.user_name}
                lst_user.append(user_dict)
        return lst_user


class ClusterService:
    """
    A service class to interact with Databricks cluster data.

    Attributes:
        client: A client object to interact with the Databricks API.
    """

    def __init__(self, client):
        """
        Initializes the ClusterService with a given client.

        Args:
            client: An instance of a Databricks client (WorkspaceClient).
        """
        self.client = client

    def list_clusters(self):
        """
        Lists all clusters in the Databricks workspace.

        Returns:
            A list of clusters available in the workspace.
        """
        return self.client.clusters.list()

    def print_cluster_names(self):
        """
        Prints the names of all clusters in the Databricks workspace.
        """
        clusters = self.list_clusters()
        for cluster in clusters:
            print(cluster.cluster_name)


class FileService:
    """
    A service class to interact with Databricks file system data.

    Attributes:
        client: A client object to interact with the Databricks API.
    """

    def __init__(self, client):
        """
        Initializes the FileService with a given client.

        Args:
            client: An instance of a Databricks client (WorkspaceClient).
        """
        self.client = client

    def list_files(self, path: str):
        """
        Lists all files in the specified directory path in the Databricks file system.

        Args:
            path: The directory path to list files from.

        Returns:
            A list of files in the specified directory.
        """
        return self.client.dbutils.fs.ls(path)

    def print_file_paths(self, path: str):
        """
        Prints the paths of all files in the specified directory path.

        Args:
            path: The directory path to print file paths from.
        """
        files = self.list_files(path)
        for file in files:
            print(file.path)


class WorkspaceUserService(UserService):
    """
    A service class to interact with Databricks workspace user data.

    Inherits from UserService.
    """

    def __init__(self):
        """
        Initializes the WorkspaceUserService with a given workspace profile.

        The profile is read from the .env file.
        """
        profile = os.getenv("WORKSPACE_PROFILE")
        super().__init__(WorkspaceClient(profile=profile))


class AccountUserService(UserService):
    """
    A service class to interact with Databricks account user data.

    Inherits from UserService.
    """

    def __init__(self):
        """
        Initializes the AccountUserService with given Azure account details.

        The host, account_id, azure_client_id, and azure_client_secret are read from the .env file.
        """
        host = os.getenv("ACCOUNT_HOST")
        account_id = os.getenv("ACCOUNT_ID")
        azure_client_id = os.getenv("AZURE_CLIENT_ID")
        azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")
        client = AccountClient(
            host=host,
            account_id=account_id,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
        )
        super().__init__(client)


def main():
    """
    The main function to demonstrate the use of WorkspaceUserService, AccountUserService,
    ClusterService, and FileService.

    It initializes services for both workspace and account, retrieves filtered users,
    lists clusters, and lists files in a directory, printing the results.
    """
    workspace_service = WorkspaceUserService()
    account_service = AccountUserService()
    cluster_service = ClusterService(client=workspace_service.client)
    file_service = FileService(client=workspace_service.client)

    try:
        workspace_users = workspace_service.get_filtered_users(
            "userName co ganesh.chandra@databricks.com"
        )
        print(f"Workspace users: {workspace_users}")

        account_users = account_service.get_filtered_users("userName co gc")
        print(f"Account users: {account_users}")

        cluster_service.print_cluster_names()

        file_service.print_file_paths("/FileStore/")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
