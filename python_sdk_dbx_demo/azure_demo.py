import os
import logging
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.service import iam

# Load environment variables from .env file
load_dotenv()

# Configure logging

# Ensure the logs directory exists
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "databricks_service.log")
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Create a console handler to display logs on the screen
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Set up basic configuration with both file and console handlers
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[file_handler, console_handler])

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

    def list_users(self, attributes: str, sort_by: str, user_filter: str, sort_order: iam.ListSortOrder):
        """
        Lists users from the Databricks workspace or account based on provided criteria.

        Args:
            attributes: The user attributes to retrieve (e.g., "id,userName").
            sort_by: The attribute to sort by (e.g., "userName").
            user_filter: The filter to apply to the user listing (e.g., "userName co gc").
            sort_order: The order in which to sort the results (ascending or descending).

        Returns:
            A list of users matching the specified criteria.

        Raises:
            Exception: If there's an error in listing users.
        """
        try:
            return self.client.users.list(
                attributes=attributes,
                sort_by=sort_by,
                filter=user_filter,
                sort_order=sort_order,
            )
        except Exception as e:
            logging.error(f"Error listing users: {e}")
            raise

    def get_filtered_users(self, user_filter: str):
        """
        Retrieves and returns a filtered list of users.

        Args:
            user_filter: The filter to apply to the user listing (e.g., "userName co gc").

        Returns:
            A list of dictionaries, each containing user id and user name.

        Raises:
            Exception: If there's an error in retrieving filtered users.
        """
        try:
            all_users = self.list_users(
                attributes="id,userName",
                sort_by="userName",
                user_filter=user_filter,
                sort_order=iam.ListSortOrder.DESCENDING,
            )
            lst_user = []
            for u in all_users:
                user_dict = {"id": u.id, "user_name": u.user_name}
                lst_user.append(user_dict)
            return lst_user
        except Exception as e:
            logging.error(f"Error getting filtered users: {e}")
            raise

    def list_groups(self):
        """
        Lists all groups in the Databricks account.

        Returns:
            A list of group display names.

        Raises:
            Exception: If there's an error in listing groups.
        """
        try:
            all_groups = self.client.groups.list()
            lst_groups = [group.display_name for group in all_groups]
            return lst_groups
        except Exception as e:
            logging.error(f"Error listing groups: {e}")
            raise


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

        Raises:
            Exception: If there's an error in listing clusters.
        """
        try:
            return self.client.clusters.list()
        except Exception as e:
            logging.error(f"Error listing clusters: {e}")
            raise

    def print_cluster_names(self):
        """
        Prints the names of all clusters in the Databricks workspace.

        Raises:
            Exception: If there's an error in printing cluster names.
        """
        try:
            clusters = self.list_clusters()
            for cluster in clusters:
                logging.info(f"Cluster name: {cluster.cluster_name}")
        except Exception as e:
            logging.error(f"Error printing cluster names: {e}")
            raise


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

        Raises:
            Exception: If there's an error in listing files.
        """
        try:
            return self.client.dbutils.fs.ls(path)
        except Exception as e:
            logging.error(f"Error listing files in {path}: {e}")
            raise

    def print_file_paths(self, path: str):
        """
        Prints the paths of all files in the specified directory path.

        Args:
            path: The directory path to print file paths from.

        Raises:
            Exception: If there's an error in printing file paths.
        """
        try:
            files = self.list_files(path)
            for file in files:
                logging.info(f"File path: {file.path}")
        except Exception as e:
            logging.error(f"Error printing file paths from {path}: {e}")
            raise


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

    Logs the operations and handles exceptions gracefully.
    """
    workspace_service = WorkspaceUserService()
    account_service = AccountUserService()
    cluster_service = ClusterService(client=workspace_service.client)
    file_service = FileService(client=workspace_service.client)

    try:
        workspace_users = workspace_service.get_filtered_users("userName co gc")
        logging.info(f"Workspace users: {workspace_users}")
        
        account_groups = account_service.list_groups()
        logging.info(f"Account Groups: {account_groups}")

        account_users = account_service.get_filtered_users("userName co gc")
        logging.info(f"Account users: {account_users}")

        cluster_service.print_cluster_names()

        file_service.print_file_paths("/FileStore/")

    except Exception as e:
        logging.critical(f"Unhandled error: {e}")


if __name__ == "__main__":
    main()
