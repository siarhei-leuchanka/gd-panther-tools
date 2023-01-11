import copy

class CheckWorkspaces:
    def __init__(self, original_source_SDK, workspaces_to_copy) -> None:
        self.workspaces_to_check = []        
        self.workspaces_in_original = []
        self.original_source_SDK = original_source_SDK
        self.workspaces_to_copy = workspaces_to_copy
        self.workspaces_in_original_meta = original_source_SDK.catalog_workspace.list_workspaces()
    
    def _parse_workspaces_to_copy(self):
        for workspaces in self.workspaces_to_copy: 
            if 'parent' in workspaces:
                if workspaces.get('parent', '') != None:                
                    self.workspaces_to_check.append(workspaces['parent'])
            if 'workspaces' in workspaces:
                if workspaces['workspaces']!= None :
                    self.workspaces_to_check +=  workspaces['workspaces']
            else:
                raise Exception("You have to provide workspaces")

    def _parse_workspace_list_objects(self):
        self.workspaces_in_original = []
        self.workspaces_in_original_to_dict = {}
        for workspace in self.workspaces_in_original_meta:   
            self.workspaces_in_original.append(workspace.id)
            self.workspaces_in_original_to_dict[workspace.id]=workspace                
        return self.workspaces_in_original_to_dict
    
    def original_workspaces_dict(self):
        return self._parse_workspace_list_objects()

    def check_workspaces_presence(self):
        self._parse_workspaces_to_copy()
        self._parse_workspace_list_objects()
        check = all(item in self.workspaces_in_original for item in self.workspaces_to_check) 
        if check is False:
            raise Exception("you made some issue(s) with workspaces IDs. Check that Workspaces you requested really exist in the source GD Cloud Instance")
            
        else:
            print("Requested workspaces are found in the original source. All good. Continue")

class CheckDataSources:
    def __init__(self, original_source_SDK, target_source_SDK) -> None:
        self.original_source_SDK = original_source_SDK
        self.target_source_SDK = target_source_SDK
    
    def data_sources_duplicated_by_id(self):
        for original_dataSource in self.original_source_SDK.catalog_data_source.list_data_sources():
            for target_dataSource in self.target_source_SDK.catalog_data_source.list_data_sources():
                print(original_dataSource.id, "!=?" ,target_dataSource.id)
                if original_dataSource.id == target_dataSource.id:
                    print("Abort data sources transition! Check your destinatation list of data sources. Target Instance has Data source with the same ID!")
                    return True
        return False
                    
class CreateWorkSpaces:
    def __init__(self, original_source_SDK, target_source_SDK, original_workspaces_dict, workspaces_to_copy, CatalogWorkspace, CatalogDeclarativeWorkspaceDataFilters) -> None:        
        self.original_source_SDK = original_source_SDK #original_workspaces_dict - > is from CheckWorkspaces.original_workspaces_dict
        self.target_source_SDK = target_source_SDK
        self.original_workspaces_dict = original_workspaces_dict
        self.workspaces_to_copy = workspaces_to_copy
        self.CatalogWorkspace = CatalogWorkspace
        self.CatalogDeclarativeWorkspaceDataFilters = CatalogDeclarativeWorkspaceDataFilters
        
    def _create_workspace(self, w_id, w_name, w_parent):
        self.target_source_SDK.catalog_workspace.create_or_update(
            self.CatalogWorkspace(
                workspace_id= w_id,
                name= w_name,
                parent_id = w_parent
            )
        )
    
    def _get_and_load_LDM_and_ADM(self, from_workspace_id, to_workspace_id):
        self.ldm_to_load, self.adm_to_load = '',''

        self.ldm_to_load = self.original_source_SDK.catalog_workspace_content.get_declarative_ldm(from_workspace_id)
        self.adm_to_load = self.original_source_SDK.catalog_workspace_content.get_declarative_analytics_model(from_workspace_id)

        self.target_source_SDK.catalog_workspace_content.put_declarative_ldm(to_workspace_id, self.ldm_to_load)
        self.target_source_SDK.catalog_workspace_content.put_declarative_analytics_model(to_workspace_id, self.adm_to_load)


    def _transfer_data_filter(self, parent_w_id, prefix):
        self.declarative_workspace_filters = self.original_source_SDK.catalog_workspace.get_declarative_workspace_data_filters().to_dict()
        
        self.workspaceDataFilters = list(self.declarative_workspace_filters["workspaceDataFilters"])
        for filter in self.workspaceDataFilters:    
            if filter['workspace']['id'] == parent_w_id:
                self.copy_filter = copy.deepcopy(filter)      
                self.copy_filter['workspace']['id'] = prefix + parent_w_id
                for workspace in self.copy_filter['workspaceDataFilterSettings']:
                    workspace['workspace']['id']=prefix + workspace['workspace']['id']
                if self.original_source_SDK != self.target_source_SDK: ### checking that we are not transferring to the same GD Cloud Instance
                    return {'workspaceDataFilters' : [self.copy_filter]}
                else:
                    return self.declarative_workspace_filters['workspaceDataFilters'].append(self.copy_filter)
            else:
                print("no data filters found! Your children workspaces may not work correctly")


    def replicate_workspaces(self):        
        for workspace in self.workspaces_to_copy:
            self.prefix = ""            
            self.w_id_parent = workspace.get('parent', '')
            
            if self.w_id_parent != '':
                self.prefix = workspace.get('prefix','')
                self._create_workspace(self.prefix + self.w_id_parent, self.prefix + self.original_workspaces_dict.get(self.w_id_parent).name, '')
                for item in workspace.get('workspaces'):
                    self._create_workspace(self.prefix + item, self.prefix + self.original_workspaces_dict.get(item).name, self.prefix + workspace.get('parent', ''))
                
                #loading LDM & ADM
                self._get_and_load_LDM_and_ADM(self.w_id_parent, self.prefix + self.w_id_parent)

                #loading Data Filters associated with parent workspace
                self.data_filter = self._transfer_data_filter(self.w_id_parent,self.prefix)
                self.target_source_SDK.catalog_workspace.put_declarative_workspace_data_filters(workspace_data_filters = self.CatalogDeclarativeWorkspaceDataFilters.from_dict(self.data_filter))

            else:
                for item in workspace.get('workspaces'):
                    self.prefix = workspace.get('prefix','')
                    self._create_workspace(self.prefix + item, self.prefix + self.original_workspaces_dict.get(item).name, '')

                    #loading LDM & ADM
                    self._get_and_load_LDM_and_ADM(item, self.prefix + item)
    



        
