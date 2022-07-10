import os
import logging
from core import logging_utils
from core import wmconstants
from core.dbclient import dbclient

class WSSettingsClient(dbclient):
    def get_wssettings_list(self):
        """ 
        Returns an array of json objects for workspace settings. 
        """
        all_result = []
        result = {}

        ws_keymap = [{"name": "enableJobViewAcls", "defn":"Prevent users from seeing jobs that they do not have access to."},
            {"name": "enforceClusterViewAcls", "defn":"Prevent users from seeing clusters that they do not have access to."},
            {"name": "enforceWorkspaceViewAcls", "defn":"Prevent users from seeing objects in the workspace file browser that they do not have access to."},
            {"name": "enableDcs", "defn":"Databricks Container Services allows users in your workspace to specify a Docker image when creating clusters."},
            {"name": "enableGp3", "defn":"When enabled, AWS EBS gp3 volumes will be used when adding additional SSD volumes to cluster instances; when disabled, AWS EBS gp2 volumes are used."},
            {"name": "enableProjectTypeInWorkspace", "defn":"Enable or disable Repos. You should see a new Repos icon in your workspace's left navigation when this feature is enabled"},
            {"name": "enableWorkspaceFilesystem", "defn":"Enable or disable Files in Repos."},
            {"name": "enableProjectsAllowList", "defn":"Enable or disable restricting commit and push operations in Repos to a configurable allow list. The allow list will be empty by default."}, 
            {"name": "intercomAdminConsent", "defn":"Allow Databricks to make suggestions to end users and turn on product tours to help with onboarding and engagement."},
            {"name": "enable-X-Frame-Options", "defn":"Sending the 'X-Frame-Options: sameorigin' response header prevents third-party domains from iframing Databricks."},
            {"name": "enable-X-Content-Type-Options","defn": "Sending the 'X-Content-Type-Options: nosniff' response header instructs browsers not to perform MIME type sniffing."},
            {"name": "enable-X-XSS-Protection", "defn":"Sending the 'X-XSS-Protection: 1; mode=block' response header instructs browsers to prevent page rendering if an attack is detected."},
            {"name": "enableResultsDownloading", "defn":"Enable or disable the download button for notebook results."},
            {"name": "enableUploadDataUis", "defn":"Enable or disable uploading data to Databricks File System (DBFS) directly from the homepage, the Data tab, and the File menu in a notebook."},
            {"name": "enableExportNotebook", "defn":"Enable or disable exporting notebooks and cells within notebooks."},
            {"name": "enableNotebookGitVersioning", "defn":"Enable or disable git versioning for notebooks."},
            {"name": "enableNotebookTableClipboard", "defn":"Enable or disable the ability of users to copy tabular data to the clipboard via the Notebooks UI."},
            {"name": "enableWebTerminal", "defn":"Enable or disable web terminal for clusters."},
            {"name": "enableDbfsFileBrowser", "defn":"Enable or disable DBFS File Browser"},
            {"name": "enableDatabricksAutologgingAdminConf", "defn":"Enable or disable Databricks Autologging for this workspace. When enabled, ML model training runs executed interactively on clusters with supported versions of the Databricks Runtime for Machine Learning will automatically be logged to MLflow."},
            {"name": "mlflowRunArtifactDownloadEnabled", "defn":"Enable or disable the downloading of artifacts logged to an MLflow run. They will still be viewable in the UI."},
            {"name": "mlflowModelServingEndpointCreationEnabled", "defn":"Enable or disable Classic model serving for this workspace. Disabling this option will not disable the existing model serving endpoints."},
            {"name": "mlflowModelRegistryEmailNotificationsEnabled", "defn":"Enable or disable model registry email notifications for this workspace."},
            {"name": "heapAnalyticsAdminConsent", "defn":"Allow Databricks to collect usage patterns to better support you and to improve the product"},
            {"name": "storeInteractiveNotebookResultsInCustomerAccount", "defn":"When enabled, all interactive notebook results are stored in the customer account."},
            {"name": "enableVerboseAuditLogs", "defn":"Enable or disable verbose audit logs"},                      
            {"name": "enableHlsRuntime", "defn":"Databricks Runtime for Genomics"}]

        for keyn in ws_keymap:
            valn = self.get("/preview/workspace-conf?keys="+keyn['name'], version='2.0')
            valins = {}
            valins['name']=keyn['name']
            valins['defn']=keyn['defn']
            valins['value']=False if valn[keyn['name']]==None else valn[keyn['name']]
            all_result.append(valins)
        return all_result

    def flatten(t):
     return [item for sublist in t for item in sublist]




