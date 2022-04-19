from core import parser as pars
from core import dbclient
from pkgcluster import ClustersClient
import sys, requests

# python 3.6

def main():
    # define a parser to identify what component to import / export
    parser = pars.get_input_items_parser()
    # parse the args
    args = parser.parse_args()
    # # parse the path location of the Databricks CLI configuration
    client_config = pars.build_client_config(args)

    # print("Test connection at {0} with profile {1}\n".format(url, args.profile))
    db_client = dbclient(client_config)
    try:
        is_successful = db_client.test_connection()

        if is_successful == 0:
            print("Connection successful!")
        else:
            print("\nUnsuccessful connection. Verify credentials.\n")
        clusterClient = ClustersClient(client_config)
        clusterLst = clusterClient.get_cluster_list(alive=False)
        clusterLst2 = clusterClient.get_cluster_acls("0127-022013-kw51wqsg", '')
        print(clusterLst2)

    except requests.exceptions.RequestException as e:
        print(e)
        print("\nUnsuccessful connection. Verify credentials.\n")
        sys.exit(1)

if __name__ == '__main__':
    main()
