
from clientpkgs.IPAccessClient import IPAccessClient
from clientpkgs.TokensClient import TokensClient
from core import logging_utils, parser as pars
from core.dbclient import dbclient
from clientpkgs.ClustersClient import ClustersClient
from clientpkgs.DbfsClient import DbfsClient
from clientpkgs.ScimClient import ScimClient
from clientpkgs.JobsClient import JobsClient
from clientpkgs.SecretsClient import SecretsClient
from clientpkgs.AccountsClient import AccountsClient
from clientpkgs.JobRunsClient import JobRunsClient
from clientpkgs.PoolsClient import PoolsClient

import sys, json, requests

# python 3.6

def main():
    
    jsonstr = '''{"profile": "", "url":"", "export_db": "logs", "is_azure":"False", "verify_ssl": "False", "verbosity":"debug", 
    "clusterid":"0225-034621-fyv7t2c","master_name_scope":"masterscp", 
    "master_name_key":"user", "master_pwd_scope":"masterscp", "master_pwd_key":"pass"}'''
    # define a parser to identify what component to import / export
    fconfig = pars.parse_dbcfg(profile='rkmtzarsrc')   #e2demofieldeng
    # parse the args
    client_config = pars.parse_input_jsonargs(jsonstr, host=fconfig['host'], token=fconfig['token'] )
    
    print(client_config['verbosity'])
    loggr = logging_utils.get_logger(__name__,client_config['verbosity'])
    loggr.debug(client_config)

    # print("Test connection at {0} with profile {1}\n".format(url, args.profile))
    db_client = dbclient(client_config)
    try:
        is_successful = db_client.test_connection()

        if is_successful == True:
            loggr.info("Connection successful!")
        else:
            loggr.info("Unsuccessful connection. Verify credentials.")

        ipaccessClient = IPAccessClient(client_config)
        iplist=ipaccessClient.get_ipaccess_list()
        print(iplist)


        tokensClient = TokensClient(client_config)
        tokensList=tokensClient.get_tokens_list()
        print(tokensList)


        # jobrunsClient = JobRunsClient(client_config)
        # lst = jobrunsClient.get_jobruns_list()
        # print(lst)

        # instancepoolsClient = PoolsClient(client_config)
        # lst = instancepoolsClient.get_pools_list()
        # print(lst)


        # acctClient = AccountsClient(client_config)
        # lst = acctClient.get_workspace_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)
        # lst = acctClient.get_credentials_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)
        # lst = acctClient.get_network_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)
        # lst = acctClient.get_storage_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)
        # lst = acctClient.get_cmk_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)
        # lst = acctClient.get_logdelivery_list('a2033dd6-73e6-465a-8898-973fbde27970')
        # print(lst)



        # clusterClient = ClustersClient(client_config)
        # clusterLst = clusterClient.get_cluster_list(alive=False)
        # clusterLst2 = clusterClient.get_cluster_acls("0121-204226-yq5nnyt7", '')
        # clusterLst3 = clusterClient.get_cluster_id_by_name('rkmjdbc')
        # clusterLst4 = clusterClient.get_global_init_scripts()
        # clusterLst5 = clusterClient.get_instance_pools()
        # clusterLst6 = clusterClient.get_spark_versions()
        # clusterLst7 = clusterClient.get_instance_profiles_list()
        # clusterLst8 = clusterClient.get_iam_role_by_cid('1011-090100-bait793')
        # clusterLst9 = clusterClient.get_policies()
        # print('spark3:' + str(clusterClient.is_spark_3('1011-090100-bait793')))
        # #clusterClient.start_cluster_by_name('at&t_test')

        # #print(clusterLst9)
        # dbfsClient = DbfsClient(client_config)
        # lst = dbfsClient.get_dbfs_mounts(cid='1011-090100-bait793')
        # #for jsonobj in lst:
        # #    print(jsonobj['path'] + "---->" + jsonobj['source'])
        # jobsClient = JobsClient(client_config)
        # joblist = jobsClient.get_jobs_list()
        # #print(joblist)
        # scimClient = ScimClient(client_config)
        # users = scimClient.get_users()
        # #print(users)
        # groups = scimClient.get_groups()
        # #print(groups)

        # secretsClient = SecretsClient(client_config)
        # scopeslist = secretsClient.get_secret_scopes_list()
   
        # #print(scopeslist)
        # for scope in scopeslist:
        #     secretslist = secretsClient.get_secrets(scope['name'])
        #     #print(secretslist)

    except requests.exceptions.RequestException as e:
        loggr.exception('Unsuccessful connection. Verify credentials.')
        sys.exit(1)
    except Exception:
        loggr.exception("Exception encountered")
if __name__ == '__main__':
    main()
