# Panther copier #0.0.2
Supported use cases:
1. Copy all workspaces from one instance to another
2. Copy workspaces with no parent. Parent workspace will be transferred automatically
3. Copy selected workspaces while its parent exists in the target instance
4. - The same for the same instance with use of defined prefix or postfix.

The script is not ready to update workspaces in the target instance, only create from scratch! Do not attemp to update existing workspaces. It could lead to unpredicted results. 

# How to:
0. Install dependencies from requirements.txt . Using pip install -r requirements.txt
1. Provide your data into yaml file and point to it in main.py. Example is stored here-> WORKSPACES_TO_COPY.yaml
1.a ORIGINAL_HOST: str, Instance FROM you copy workspaces
    - ORIGINAL_HOST_TOKEN: str, Token for the Instance FROM which you copy workspaces
    - TARGET_HOST: str, Host of the Target instance. Leave blank string ('') if go with the same GD Cloud Instance to replicate workspaces
    - TARGET_HOST_TOKEN: str, Token of the Target instance. Leave blank if go with the same GD Cloud Instance
    - COPY_DB_SOURCE: boolen (True if you want to transfer data source definitions, False if not. Should be false if copying to the same instance)
    - WORKSPACES_TO_COPY: list, E.g, ['ecommerce-parent', 'ecommerce-parent1']
    - PREFIX_FOR_NEW_WORKSPACES: str, Prefix for IDs and Workspaces Names. Leave empty string ('') if you no need. (!) Mandatory to replicate in the same environment, otherwise it will try to update the existing workspaces!!!
2. Run main.py
3. - If DB sources definitions were transfered -> provide credentials in the target instance. 

