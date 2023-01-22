from gooddata_sdk import GoodDataSdk, CatalogWorkspace, CatalogDeclarativeWorkspaceDataFilters
import copyWorkspacesClasses as cl
import ruamel.yaml


# Reading instructions for workspaces to copy
# ! remove the second copy in the name of yaml file
with open('WORKSPACES_TO_COPY copy.yaml', 'r') as File:
    config_data = ruamel.yaml.load(File, Loader=ruamel.yaml.Loader)

WORKSPACES_TO_COPY= config_data['WORKSPACES_TO_COPY']
prefix = config_data.get('PREFIX_FOR_NEW_WORKSPACES', '')

# creating SDK instances
try:
    original_source_SDK = GoodDataSdk.create(config_data['ORIGINAL_HOST'], config_data['ORIGINAL_HOST_TOKEN'])
    if config_data['TARGET_HOST'] != "" and config_data['TARGET_HOST_TOKEN'] != "":
        target_source_SDK = GoodDataSdk.create(config_data['TARGET_HOST'], config_data['TARGET_HOST_TOKEN'])
        print("Copying to the " + config_data['TARGET_HOST'])
    else:
        target_source_SDK = original_source_SDK
        print("Copying to the same HOST")
except:
    print("Check credentials. Something is wrong with your SDK instance creation")


#checking that workspaces IDs are really existing in the original source
meta_data_check = cl.CheckInputs(original_source_SDK, target_source_SDK, WORKSPACES_TO_COPY)
print("Checking Workspaces: ", WORKSPACES_TO_COPY)
if meta_data_check.valid_workspaces() == False:
    raise Exception("You made a mistake in a list of workspaces to copy")


# Checking DB Sources and Copying if needed. 
if config_data['COPY_DB_SOURCE'] == True:
    #checking that Data Sources to Copy do not exist
    if meta_data_check.data_sources_duplicated_by_id() == False:
        #starting duplication of data sources
        print("Starting transfer of data sources")        

        for dataSource in meta_data_check.data_sources:
            declarative_data_source = original_source_SDK.catalog_data_source.get_data_source(dataSource)
            target_source_SDK.catalog_data_source.create_or_update_data_source(declarative_data_source)  
            #adding PDM
            pdm = original_source_SDK.catalog_data_source.get_declarative_pdm(dataSource)        
            target_source_SDK.catalog_data_source.put_declarative_pdm(dataSource, pdm)
        print("Success! Make sure to provide User and Password for you DB in the target instance." ) 



# # Creating Workspaces taking into account hierarcy
transfer = cl.WorkspacesProcurement(original_source_SDK, target_source_SDK)        

created_workspaces = [] # tracking what has been added and in what order
for w in WORKSPACES_TO_COPY:    
    workspaces_with_parents = transfer.restore_hierarchy(w)
    workspaces_with_parents.reverse()  #starting from Parent! 
    for workspace in workspaces_with_parents:
        if workspace not in created_workspaces:
            print('creating workspace ->> ', workspace)            
            created_workspaces.append(transfer.create_workspace(CatalogWorkspace, workspace, prefix))

print("Created_workspaces ->", created_workspaces)

# loading LDM & ADM 
for parent in transfer.get_parents_workspaces:
    print("loading LDM & ADM for Parent -> ", parent)
    transfer.get_and_load_LDM_and_ADM(parent,config_data.get('PREFIX_FOR_NEW_WORKSPACES', ''))


# loading Data Filters
data_filters = transfer.transfer_data_filters(created_workspaces, prefix)
target_source_SDK.catalog_workspace.put_declarative_workspace_data_filters(workspace_data_filters = CatalogDeclarativeWorkspaceDataFilters.from_dict(data_filters))



