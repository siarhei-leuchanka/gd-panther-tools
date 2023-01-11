from gooddata_sdk import *
import copyWorkspacesClasses as cl
import ruamel.yaml


# Reading instructions for workspaces copy
with open('WORKSPACES_TO_COPY.yaml', 'r') as File:
    config_data = ruamel.yaml.load(File, Loader=ruamel.yaml.Loader)

WORKSPACES_TO_COPY= config_data['WORKSPACES_TO_COPY']


# creating SDK instances
try:
    original_source_SDK = GoodDataSdk.create(config_data['ORIGINAL_HOST'], config_data['ORIGINAL_HOST_TOKEN'])
    if config_data['TARGET_HOST'] != "" and config_data['TARGET_HOST_TOKEN'] != "":
        target_source_SDK = GoodDataSdk.create(config_data['TARGET_HOST'], config_data['TARGET_HOST_TOKEN'])
    else:
        target_source_SDK = original_source_SDK
except:
    print("Check credentials. Something is wrong with your SDK instance creation")

#checking that workspaces IDs are really existing in the original source
workspace_check = cl.CheckWorkspaces(original_source_SDK,WORKSPACES_TO_COPY)
workspace_check.check_workspaces_presence()

# Taking care of DB Sources
if config_data['COPY_DB_SOURCE'] == True:
    #checking that Data Sources to Copy do not exist 
    if cl.CheckDataSources(original_source_SDK, target_source_SDK).data_sources_duplicated_by_id() == False:
        #starting duplication of data sources
        print("Starting transfer of data sources")
        declarative_data_sources = original_source_SDK.catalog_data_source.get_declarative_data_sources() 

        #### !!! for now skipping as layout load requires all user groups. 
        #target_source_SDK.catalog_data_source.put_declarative_data_sources(declarative_data_sources,'creds.yaml')

        for dataSource in original_source_SDK.catalog_data_source.list_data_sources():
            target_source_SDK.catalog_data_source.create_or_update_data_source(dataSource)  
            #adding PDM
            pdm = original_source_SDK.catalog_data_source.get_declarative_pdm(dataSource.id)        
            target_source_SDK.catalog_data_source.put_declarative_pdm(dataSource.id, pdm)
        print("make sure to provide User and Password for you DB in the target instance." ) 


# Replicating workspaces
replica = cl.CreateWorkSpaces(original_source_SDK, target_source_SDK, workspace_check.original_workspaces_dict(), WORKSPACES_TO_COPY, CatalogWorkspace, CatalogDeclarativeWorkspaceDataFilters)
replica.replicate_workspaces()
