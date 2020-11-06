#! /bin/bash
az group create -g databricks -l westus
#az deployment group create -g databricks  --template-file databrick-scc-deploy.json
az deployment group create -g databricks  --template-file databrick-scc-deploy.json --parameters deploy.parameters.json

