from gooddata_sdk import GoodDataSdk, CatalogWorkspace, CatalogDeclarativeWorkspaceDataFilters
import copyWorkspacesClasses as cl
import ruamel.yaml
import sys


# Reading instructions for workspaces to copy
# ! remove the second copy in the name of yaml file
with open('WORKSPACES_TO_COPY copy.yaml', 'r') as File:
    config_data = ruamel.yaml.load(File, Loader=ruamel.yaml.Loader)

WORKSPACES_TO_COPY= config_data['WORKSPACES_TO_COPY']
prefix = config_data.get('PREFIX_FOR_NEW_WORKSPACES', '')
postfix = config_data.get('POSTFIX', '')

# creating SDK instances
try:
    original_source_SDK = GoodDataSdk.create(config_data['ORIGINAL_HOST'], config_data['ORIGINAL_HOST_TOKEN'])
    if config_data['TARGET_HOST'] != "" and config_data['TARGET_HOST_TOKEN'] != "":
        target_source_SDK = GoodDataSdk.create(config_data['TARGET_HOST'], config_data['TARGET_HOST_TOKEN'])
        print("Copying to the " + config_data['TARGET_HOST'])
    else:
        if prefix != '' or postfix != '':
            target_source_SDK = original_source_SDK
            print("Copying to the same HOST")
        else:
            print("You need to provide Prefix or Postfix!")
            sys.exit(1)

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
            created_workspaces.append(transfer.create_workspace(CatalogWorkspace, workspace, prefix, postfix))

print("Created_workspaces ->", created_workspaces, " Added Prefix {} . Postfix {}".format(prefix,postfix))

# loading LDM & ADM 
for parent in transfer.get_parents_workspaces:
    print("loading LDM & ADM for Parent -> ", parent)
    transfer.get_and_load_LDM_and_ADM(parent, prefix, postfix)


### Preparing & loading Data Filters

extracted_data_filters = transfer.extract_data_filters(created_workspaces,prefix,postfix)

# extracting existing data filters from target instance
target_data_filters = target_source_SDK.catalog_workspace.get_declarative_workspace_data_filters().to_dict()['workspaceDataFilters']

# getting list of ids to check the Collision Course 
# (c) JAY-Z, Linkin Park ~♫♬ ♫♫ ♪♩ ♬♬ ♬♬ 
extracted_data_filters_id = [i['id'] for i in extracted_data_filters]
target_data_filters_id = [i['id'] for i in target_data_filters]

collisions =[   id     for id in target_data_filters_id     if id in extracted_data_filters_id   ]

if collisions == []:
    target_source_SDK.catalog_workspace.put_declarative_workspace_data_filters(workspace_data_filters = CatalogDeclarativeWorkspaceDataFilters.from_dict({'workspaceDataFilters' : extracted_data_filters + target_data_filters}))    
else:
    for filter in target_data_filters:
        if filter['id'] in collisions:
            for extracted_filter in extracted_data_filters:
                if extracted_filter['id'] == filter['id']:
                    extracted_filter['workspaceDataFilterSettings'].extend(filter['workspaceDataFilterSettings'])                
        else:
            extracted_data_filters.append(filter)    
    target_source_SDK.catalog_workspace.put_declarative_workspace_data_filters(workspace_data_filters = CatalogDeclarativeWorkspaceDataFilters.from_dict({'workspaceDataFilters' : extracted_data_filters}))        




