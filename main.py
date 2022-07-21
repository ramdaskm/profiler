
from clientpkgs.IPAccessClient import IPAccessClient
from clientpkgs.TokensClient import TokensClient
from clientpkgs.WorkspaceClient import WorkspaceClient
from core import  parser as pars
from core.logging_utils import LoggingUtils
from core.dbclient import dbclient
from clientpkgs.ClustersClient import ClustersClient
from clientpkgs.DbfsClient import DbfsClient
from clientpkgs.ScimClient import ScimClient
from clientpkgs.JobsClient import JobsClient
from clientpkgs.SecretsClient import SecretsClient
from clientpkgs.AccountsClient import AccountsClient
from clientpkgs.JobRunsClient import JobRunsClient
from clientpkgs.PoolsClient import PoolsClient
from clientpkgs.WSSettingsClient import WSSettingsClient
from clientpkgs.InitScriptsClient import InitScriptsClient
from clientpkgs.LibrariesClient import LibrariesClient

import sys, json, requests

# python 3.6

def main():
    ##"account_id": "a2033dd6-73e6-465a-8898-973fbde27970" "clusterid":"0606-201442-d3auaeu4" - db-sme-demo-rkm-mgtz-2
    ##"account_id": "119f3ee2-8c38-4cdb-88e1-81c091c378a2" "clusterid":"0525-215202-rguwsnht" e2-demo-migrate-src.cloud.databricks.com
    # jsonstr = '''{"profile": "", "url":"", "account_id": "a2033dd6-73e6-465a-8898-973fbde27970", "export_db": "logs", "is_azure":"False", "verify_ssl": "False", "verbosity":"debug", 
    # "clusterid":"0606-201442-d3auaeu4","master_name_scope":"masterscp", 
    # "master_name_key":"user", "master_pwd_scope":"masterscp", "master_pwd_key":"pass"}'''
    
    
    jsonstr = '''{"profile": "", "url":"", "account_id":"dcdbb945-e659-4e8c-b108-db6b3ac3d0eb", "export_db": "logs", "is_azure":"False", "verify_ssl": "False", "verbosity":"info", 
  "clusterid":"0720-140418-zufw1e6x","master_name_scope":"swat_masterscp", 
  "master_name_key":"user", "master_pwd_scope":"swat_masterscp", "master_pwd_key":"pass",
      "workspace_pat_scope":"swat_masterscp",  "workspace_pat_token":"sat_token" }'''

    # define a parser to identify what component to import / export
    fconfig = pars.parse_dbcfg(profile='sfe')   #e2demofieldeng rkmtzar  rkmtzarsrc sfe
    # parse the args
    client_config = pars.parse_input_jsonargs(jsonstr, host=fconfig['host'], token=fconfig['token'] )
    
    print(client_config['verbosity'])
    loggr = LoggingUtils.get_logger()
    loggr.debug(client_config)

    # print("Test connection at {0} with profile {1}\n".format(url, args.profile))
    db_client = dbclient(client_config)
    try:
        is_successful = db_client.test_connection()

        if is_successful == True:
            loggr.info("Connection successful!")
        else:
            loggr.info("Unsuccessful connection. Verify credentials.")




        workspaceClient = WorkspaceClient(client_config)
        # notebookList = workspaceClient.get_all_notebooks()
        #notebookList = workspaceClient.get_list_notebooks('/Repos/ramdas.murali+tzar@databricks.com/CSE/gold/workspace_analysis/dev')
        # print(notebookList)
        # libClient = LibrariesClient(client_config)
        # libList = libClient.get_libraries_status_list()
        # print(libList)

        # secretsClient = SecretsClient(client_config)
        # scopeslist = secretsClient.get_secret_scopes_list()

        # secretslist = secretsClient.get_secrets(scopeslist)
        # print(secretslist)

        #acctClient = AccountsClient(client_config)
        #lst = acctClient.get_workspace_list()
        #lst = acctClient.get_privatelink_info()
        #print(lst)
        # initClient = InitScriptsClient(client_config)
        # scriptsList = initClient.get_allglobalinitscripts_list()
        # print(scriptsList)



        # dbfsclient = DbfsClient(client_config)
        # dirlist = dbfsclient.get_dbfs_directories('/user/hive/warehouse/')
        # print(dirlist)

        # dbfsmounts = dbfsclient.get_dbfs_mounts()
        # print(dbfsmounts)



        # wsclient = WSSettingsClient(client_config)
        # tokensList=wsclient.get_wssettings_list()
        # print(tokensList)


        # ipaccessClient = IPAccessClient(client_config)
        # iplist=ipaccessClient.get_ipaccess_list()
        # print(iplist)


        # tokensClient = TokensClient(client_config)
        # tokensList=tokensClient.get_tokens_list()
        # print(tokensList)


        # jobrunsClient = JobRunsClient(client_config)
        # lst = jobrunsClient.get_jobruns_list()
        # print(lst)

        # instancepoolsClient = PoolsClient(client_config)
        # lst = instancepoolsClient.get_pools_list()
        # print(lst)


        # acctClient = AccountsClient(client_config)
        # lst = acctClient.get_privatelink_info()
        # lst = acctClient.get_workspace_list()
        # print(lst)
        # lst = acctClient.get_credentials_list()
        # print(lst)
        # lst = acctClient.get_network_list()
        # print(lst)
        # lst = acctClient.get_storage_list()
        # print(lst)
        # lst = acctClient.get_cmk_list()
        # print(lst)
        # lst = acctClient.get_logdelivery_list()
        # print(lst)



        clusterClient = ClustersClient(client_config)
        clusterLst = clusterClient.get_cluster_list(alive=False)
        print(clusterLst)
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
