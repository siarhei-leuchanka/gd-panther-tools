import copy
from gooddata_sdk import GoodDataSdk

class CheckInputs:
    def __init__(self, original_source_SDK: GoodDataSdk, target_source_SDK: GoodDataSdk, workspaces_to_copy) -> None:
        self.original_source_SDK = original_source_SDK        
        self.target_source_SDK = target_source_SDK
        self.original_list_workspaces = self.original_source_SDK.catalog_workspace.list_workspaces()
        self.workspaces_to_copy = workspaces_to_copy
        

    def valid_workspaces(self) -> bool:        
        workspace_ids_original_host = [w.id for w in self.original_list_workspaces]        
        check = all(item in workspace_ids_original_host for item in self.workspaces_to_copy)        
        return True if check == True else False            
    
    
    def _get_data_sources(self):
        data_sources = []
        for workspace in self.workspaces_to_copy:            
            ldm = self.original_source_SDK.catalog_workspace_content.get_declarative_ldm(workspace).to_dict()
            for i in ldm['ldm']['datasets']:
                if i['dataSourceTableId']['dataSourceId'] not in data_sources:
                    data_sources.append(i['dataSourceTableId']['dataSourceId'])
        return data_sources


    def data_sources_duplicated_by_id(self) -> bool:
        original_data_sources = self._get_data_sources()
        target_data_sources = [i.id for i in self.target_source_SDK.catalog_data_source.list_data_sources()]
        
        check = any(item in original_data_sources for item in target_data_sources)
        if check:
            print("Abort data sources transition! Check your destinatation list of data sources. Target Instance has Data source with the same ID!")
            return True
        else:
            print("No duplicates are found")
            return False


    @property
    def data_sources(self):        
        return self._get_data_sources()


class WorkspacesProcurement:
    def __init__(self, original_source_SDK: GoodDataSdk, target_source_SDK: GoodDataSdk) -> None:
        self.original_source_SDK = original_source_SDK        
        self.target_source_SDK = target_source_SDK
        self.original_list_workspaces = self.original_source_SDK.catalog_workspace.list_workspaces()
        self.collected_parents_workspaces = []
        
        self.declarative_workspace_filters = self.original_source_SDK.catalog_workspace.get_declarative_workspace_data_filters().to_dict()        
        self.workspaceDataFilters = list(self.declarative_workspace_filters["workspaceDataFilters"])
        
        self.target_list_workspaces = self.target_source_SDK.catalog_workspace.list_workspaces()
        self.workspace_ids_target_host = [w.id for w in self.target_list_workspaces]        
        

    def workspace_info(self,workspace_id):
        w = self.original_source_SDK._catalog_workspace.get_workspace(workspace_id)
        return  {
            'id': workspace_id,
             'name': w.name,
             'parent': w.parent_id
        }


    def create_workspace(self, CatalogWorkspace, workspace_id, prefix) -> str or None:
        self.info = self.workspace_info(workspace_id)     
        self.name = self.info['name']
        self.parent = self.info['parent']
        
        if prefix+workspace_id not in self.workspace_ids_target_host:            
            # if parent exists - do not touch it
            
            if self.parent is not None:
                self.parent = prefix+self.parent
            else:
                self.parent = ''
                self.collected_parents_workspaces.append(workspace_id)
            
            self.target_source_SDK.catalog_workspace.create_or_update(
                CatalogWorkspace(
                    workspace_id = prefix + workspace_id,
                    name = prefix + self.name,
                    parent_id = self.parent
                )
            )
            return workspace_id
        else:
            print(workspace_id, "-> with added prefix has been already in the target host. Skipping it's creation ")

        print("Collected Parent Workspaces ->", self.collected_parents_workspaces)


    def get_parent(self, workspace_id, instance=None):
        if instance == 'target':
            self.workspace = self.target_source_SDK.catalog_workspace.get_workspace(workspace_id)    
        if instance is None or instance == 'original':
            self.workspace = self.original_source_SDK.catalog_workspace.get_workspace(workspace_id)
        return self.workspace.parent_id


    def restore_hierarchy(self,workspace_id):
        self.catalog = []
        self.catalog.append(workspace_id)
        self.parent = self.get_parent(workspace_id)            
        while self.parent is not None:    
            self.catalog.append(self.parent)
            self.parent = self.get_parent(self.parent)
        return self.catalog


    def get_and_load_LDM_and_ADM(self, from_workspace_id, prefix) -> None:
        self.ldm_to_load, self.adm_to_load = '',''

        self.ldm_to_load = self.original_source_SDK.catalog_workspace_content.get_declarative_ldm(from_workspace_id)
        self.adm_to_load = self.original_source_SDK.catalog_workspace_content.get_declarative_analytics_model(from_workspace_id)

        self.target_source_SDK.catalog_workspace_content.put_declarative_ldm(prefix + from_workspace_id, self.ldm_to_load)
        self.target_source_SDK.catalog_workspace_content.put_declarative_analytics_model(prefix + from_workspace_id, self.adm_to_load)
    

    def extract_data_filters(self, workspaces:list, prefix:str, postfix:str) -> list:
        self.output = []
        
        for dataFilter in self.workspaceDataFilters:
            dataFilter_copy = copy.deepcopy(dataFilter)
            dataFilter_copy['workspaceDataFilterSettings'] = []
            for filterValues in dataFilter['workspaceDataFilterSettings']:
                if filterValues.get('workspace')['id'] in workspaces:
                    filterValues_copy = copy.deepcopy(filterValues)
                    filterValues_copy['id'] = prefix + filterValues_copy['id'] + postfix
                    filterValues_copy['workspace']['id'] = prefix + filterValues_copy['workspace']['id'] + postfix
                    
                    dataFilter_copy['workspaceDataFilterSettings'].append(filterValues_copy)
            if dataFilter_copy['workspaceDataFilterSettings'] == []:
                del dataFilter_copy
            else:
                self.output.append(dataFilter_copy)
                dataFilter_copy['id'] = prefix + dataFilter_copy['id'] + postfix
                dataFilter_copy['workspace']['id'] = prefix + dataFilter_copy['workspace']['id'] + postfix


        return self.output
    
    @property
    def get_parents_workspaces(self):
        return self.collected_parents_workspaces
