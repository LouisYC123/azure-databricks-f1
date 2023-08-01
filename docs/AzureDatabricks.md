# Intro to Azure Databricks


## Starting a new project

- From Azure home page, click 'Create a resource'  
- Create Azure Databricks resource  
    - Create a resource group to collect all resources (Databricks, Datafactory etc) for your project in one place  
    - Create a workspace, region and pricing tier for your project  
- create a dashboard for your project and pin your workspace to this dashboard
- Launch and navigate to Databricks workspace 

Your workspace contains all your folders libraries and files.
Each user has their own workspace, but there is also a shared workspace for sharing with other users.



### Databricks Cluster Types 

A cluster is a collection of Virtual Machines
In a cluster, there is a driver node, and multiple worker nodes
We can run different types of workloads, including ETL for data engineering, data science and machine learning workloads
Databricks offers two types of clusters:
- All Purpose
- Job Cluster

All Purpose
- created manually via UI, CLI or API
- Persistent, can be created or terminated at any time
- suitable for interactive and Ad Hoc workloads
- can be shared among many users, good for collaborative analysis
- expensive compared to Job cluster


Job Cluster 
- Created by jobs when a job starts to execute and has been configured to create job clusters
- terminated at the end of the job
- suitable for automated workloads such as running an ETL pipeline
- isolated and just for the job


It can take around 5-6 minutes to spin up a cluster
Cluster Pools allow you to use some reserved cluster capacity, which are a faster way to spin up clusters.
Cluster Policies help us to pre-configure some of the cluster configurations, so we can save them and reuse them when spinning up new clusters.
We can set a max-size in a Cluster Policy to help keep costs under control.


You can create jobs via the Workflows button on the left pane.
The job cluster will be terminated as soon as the job completes.


### Cluster Configuration

Single/Multi Node  

Multi Node will have one driver node and one or more worker nodes (default)
Single will have only one driver node and NO worker node
here, the driver node acts as both the driver node and the worker node.
mainly used for lightweight analytical workloads that dont require lots of compute

Access Mode
single user (all languages)
shared (python and sql only)
No isolation shared (all languages) - no isolation, so a fail in one users’ process may affect the others. 


Databricks Runtime Version
Databricks Runtime - optimised version of Apache Spark library - Java, Scala, Python , R, Ubuntu, GPU, Delta Lake libraries, Notebooks
Databricks Runtime ML - all of the above, plus popular ML libraries such as PyTorch, Keras, TensorFlow etc
Photon Runtime - everything from Databricks Runtime plus photon engine (engine that runs SQL faster)
Databricks Runtime Light - Only for jobs requiring advanced features and improved performance. Only suited for automated workloads. Cant be used for interactive workloads or notebooks.

Auto Termination
helps avoid unnecessary costs by terminating a cluster after x minutes.

Auto Scaling
define min and max and auto scaling will optimise between these paramters


Cluster VM Type/ Size
Memory Optimised - good for ML that caches a lot of data
compute optimised - 
storarge optimised - good when you need high disk throughput and I/O
general purpose
GPU accelerated - good for deep learning models

Cluster Policy
You can choose a preset cluster policy that predefines cluster settings.
takes away the need for data engineers to think about architecture stuff.

## Creating a cluster

From the Databricks homepage, click 'Compute' in the left hand pane, then select 'Create Compute'

In the runtime version dropdowns - 'LTS' stands for 'Long Term Support' and are supported for around two years. Select these for production workloads. The others can be used for testing and development.

Photon is the highly performant engine used for performance boosts. Clusters with Photon engines are normally more expensive but for larger workloads you may save costs as the queries finish quicker and terminate the cluster sooner.

You can set any Environment variables under the 'Advanced Options' tab.  
You can define init/bootstrap scripts to install any dependencies in the cluster under the 'init scripts' tab of the 'Advanced Options'.  
Once started, you can only edit a few settings, and most will require a restart of the cluster.  
You can view any notebooks for the cluster under the 'Notebooks' tab of the cluster view.    
You can also add libraires to your cluster from the 'Libraries' tab of the cluster view.    


## Cluster Pricing & Cost Control

https://azure.microsoft.com/en-gb/pricing/details/databricks/  

- Budget Alerts
    ```Azure Portal -> cost management -> budgets -> + Add```


## Cluster Policy

Policies are basically a set of rules which allows us to hide or take out the configuration options, that are not required on the user interface.  
They allow us to fix the values of some of the configuration parameters and restrict access for the users so that they're not changing them.  
Also, in a set of values, a policy allows us to select default values on the user interface. 
An administrator can create a policy and assign that to a set of users. When the user chooses the policy, they'll now have a Simpler User Interface because some of the options are now hidden or pre-populated with fixed values.  
Only allowing the users to select certain type of nodes or setting Auto Termination by Default, allows us to keep the cost down.  
Cluster policies take away the need for an administrator to be involved in every cluster creation decision and empowers standard users to create them.  
Cluster policies are only available for workspaces created in premium tier.  

To create a cluster policy:  
```Databricks -> Compute -> select policy / create policy```

There are some optional default policies created by Databricks for us or we can create our own either from scratch or by modifying the defaults.

Definitions are written in JSON. 
Walkthrough:  https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/35298768#questions


## Notebooks

Databricks -> Workspace -> folder -> create notebook  
```file -> clone``` creates a duplicate of the notebook  
When exporting your notebook, ```DBC``` is a DatabricksBinary file format which you can use to export your notebook.  


### Magic Commands

The primary purpose of the Magic Commands, is to override the default language in a Notebook.  
For example, you may want to switch from Python to Scala within the same notebook.  
There are also some auxiliary magic commands which allow us to create documentation in our notebooks and access the file system, etc  

For example, you can switch to SQL by 

```
%sql
SELECT 'hello'
```

```%md``` lets you switch to markdown to add some documentation to your code  

```%fs``` lets you switch to the context of the default file system.   
for example ```%fs ls``` lets you for all files in the file system  

``` %sh ``` is the shell command.  
For examle ``` %sh ps ``` lets you view all the processes that are running.  

You can also use the language button inside a cell to switch languages   
Example:  https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27514662#questions  


### Databricks Utilities

Databricks Utilities make it easier to combine different types of tasks in a Single notebook.  
For example, they allow us to combine file operations with ETL tasks.  
These utilities can only be run from Python, Scala or R cells in a Notebook. They cannot be run from a SQL cell.  

examples include:

- File system utilities
- secrets utilities
- widget utilities
- Notebook workflow utilities  

walkthrough: https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27514664#questions  

Databricks has some datasets you can use for practice projects located in /databricks-datasets/

You can also you use the dbutils package:
``` 
for files in dbutils.fs.ls('databricks-datasets/COVID'):
    if files.name.endswith('/):
        print(files.name)
```

```dbutils.help()``` will give you a list of functionality



## Azure Data Lake

### Data Lake authentication
In Azure Databricks, we generally use Azure Data Lake Storage Gen2 as the Data Lake storage solution.  
There are a number of ways Databricks can authenticate to Azure Data Lake Storage Gen2. Each Azure storage account comes with an Access Key, that we can use to access the storage account.  
Also, we can generate a special kind of key called Shared Access Signature or otherwise referred to as SAS token, and we can use that to access the storage account. SAS tokens lets us manage access at a more granular level than the Access Key. We can also create a Service Principle and give the required access for the Data Lake to the Service Principle and use those credentials to access the storage account.  
All of these three options can take two forms. The first one is to use these credentials in the notebook and authenticate to the Data Lake. The authentication in this scenario will be valid just for the duration of the session, i.e. until the notebook has been detached to the cluster. This is called Session Scoped Authentication. The other option is to use these credentials in the Cluster and authenticate from the Cluster. The authentication will happen when the Cluster starts and it will be valid until the Cluster has been terminated.  All the notebooks connected to this Cluster will have access to the data. This is called Cluster Scoped Authentication.  
Apart from these, there are two more forms of authentication patterns available in Databricks.  
First one is called the AAD Pass-through authentication, or otherwise referred to as the Azure Active Directory Pass-through authentication.
In this pattern we just need to enable the Cluster to use, Azure Active Directory Pass-through authentication.  Whenever a user runs a Notebook, the Cluster will use the user's Azure Active Directory credentials and look for the roles, the user has been assigned to the Azure Data Lake Storage using IAM or Identity and Access management.  If the user has access to the storage account, it will allow the user to access the storage account. Otherwise, the user won't be able to access the storage. AAD Pass-through authentication is only available on premium workspaces.  
The last one is the most recent addition to Databricks called Unity Catalog. In this access pattern, the administrators can define the access permissions, for an user using the Databricks Unity Catalog. When an user is trying to access the storage account, the Cluster will check for the user's access in the Unity catalog, if the user has the required permissions, it will allow the user to access the storage account, otherwise they won't be able to access. Again, this is only available on premium workspaces.

#### Setting up Azure Storage

'Create Resource' -> Storage Account -> create containers (buckets)

You can download and use the 'Microsoft Azure Storage Explorer' for convienient viewing of you storage account.

### Azure Data Lake Authentication

#### Authentication using Access Keys
When you create an Azure storage account, Azure generates two 512 bit storage account access keys. The access keys give full access to the storage account. Someone with the access key can perform everything a Owner is able to do. So consider this as a super user and you should carefully secure these access keys. Azure recommends securing them using Azure Key Vault.  

In scenarios where a key is compromised, you can rotate or regenerate the new keys. Having two separate keys helps achieve uninterrupted access to the services. Applications can use one of the keys while the other key is being regenerated.  

In order to access the data from Databricks, we need to provide one of the access keys to Azure Databricks, so that it can authenticate to the ADLs Gen2 service. We can do this by assigning the access key, to a Spark configuration called fs.azure.account .key.  
An example spark command for this would look like this:   
```
spark.conf.set(
    "fs.azure.account.key.mystorageaccount.dfs.core.windows.net",    # <- fs.azure.account.key followed by the endpoint of the storage account.
    "387oiuhJdfjf397+ipkjdLi8364j+ppPheneum7jhasN+cejuuhgrdt$jk+odjP+pks+GGfen7f+jsb6G+Dehw5=="    # <- access key
)
```

Microsoft and Databricks, recommends using the ABFS or the Azure Blob File System driver, to access data stored in a storage account from Databricks. It's a new driver, which is part of the Apache Hadoop suite of products, and it's optimized for big data analytics workloads and offers better security.  
You can read about the ABFS here: https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver 
ABFS is a more secure option than HTTPS. 
(The second s is optional but more secure as the data will be encrypted while at transit.)
```abfss://<container_name><@storage_account_name.dfs.core.windows.net/><optional_folder_name><optional_filename>```  
So in summary, in order to access the data in a Data Lake, we need to set the Spark configuration, fs.azure.account.key, with the access key of the storage account, as shown in the ```spark.conf.set()``` shown above, and then we can use the ABFS driver to access the data as shown below:
```dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net/")```

**Authenticating from a Notebook**
(Not best practice as your key is visible in the notebook and you might risk uploading it to GitHub - use Azure Key Vault instead)  
1. Get your access keys:  
Head to your storage account resource -> (left hand menu) Access Keys -> copy your access key  
2. Set your spark.conf:  
```spark.conf.set(
    ""fs.azure.account.key.mystorageaccount.dfs.core.windows.net",",
    "<paste_key_here>"
)
```
But the problem with Access Keys is that it gives full access to the entire storage account, which is not something you would want generally. You would want to restrict access and provide only the required access for your users. For example, you might want to provide only Read access to your users and also you might want to provide Access only for a limited period of time. That's where SAS tokens or Shared Access Signatures come in.  

#### Authentication using SAS Token 

SAS = Shared Access Signatures

Unlike Access keys, Shared Access Signatures can be used to control access at a more granular level. We can restrict access to specific resource types or services. For example, we can allow access to only Blob containers, thus restricting access to files, queues, tables, etc. Also, we can allow only specific permissions. For example, we can allow Read only access to a container, thus restricting the user from writing or deleting the Blobs. It even allows us to restrict the time period during which the user has access to the data. And also, we can allow access to specific IP addresses, thus avoiding public access. With this level of fine grained access control offered, SAS tokens are the recommended access pattern for external clients, who cannot be trusted with full access to your storage account.  

In order to access the data from Databricks using SAS token, we need to provide the Shared Access Signature to Azure Databricks, so that it can authenticate to the ADLs Gen2 service using the token.  

First generate a SAS token.
navigate to your storage account -> select containers (if you want the SAS token to grant permission for only a specific container) -> select the vertical elipsis for the container of choice -> Generate SAS -> specify permissions -> you can also optionally specify IP address you want to restrict access to 

You can also generate a SAS token from the 'Microsoft Azure Storage Explore' desktop app.  

Then set some ```spark.conf.set()``` parameters in your notebook:  

define the authentication type as SAS or Shared Access Signature:  
```spark.conf.set(
    "fs.azure.account.auth.type.<my-storage-account>.dfs.core.windows.net",
    "SAS
)  
```
defines the SAS token provider to fixed SAS token provider:  
```
spark.conf.set(
    "fs.azure.sas.token.provider.type<my-storage-account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)
```
set the value of the SAS token:  
```
spark.conf.set(
    "fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net",
    "<token>"
)
```


#### Authentication using Service Principal

Service Principal are quite similar to user accounts. They can be registered in the Azure Active Directory and assigned permissions required to access the resources in the Azure subscription, via Role-Based Access Control or RBAC. Azure makes a number of standard roles available and you can also create custom roles. Service principles are the recommended method to be used in automated tools such as Databricks Jobs, as well as in CI/CD pipelines. This is because they provide better security and traceability. In a good Architecture, each application will have its own service principle, and the service principal is just assigned the required permissions on the resources, therefore following the principle of least privilege. All of this can be audited and monitored, so this provides better security and monitoring. Using service principal lets you store the credentials in the Key Vault and also mount the storage to the workspace

Steps:

1. First we need to register the Service Principal. Service Principal is also referred to as, Azure AD application or Active Directory application. Each application that's registered is given a unique identifier called an application or a client ID.

2. We also need to create a secret for the Service Principal.

3. We then need to configure Databricks to access the storage account via the Service Principal. We can do that by setting some Spark configuration parameters.

4. We then need to assign the required role on the Data Lake for the Service Principal so that the Service
Principal can access the data in the Data Lake.
( you can use Storage Blob Data Reader if your application only needs a Read Only Access.)



- Step 1: Register the Service Principal  
Azure Portal -> search for Azure Active Directory. -> App Registrations -> New registrations -> +New Registration -> Give your App a name -> select account type (there is a link to help you choose) -> Register -> Pin this to your dashboard so you can easily find it -> Copy your App ID and your Directory ID -> add these to your notebooks

- Step 2: Create a secret
From you registered app page, got to 'Certificates & Secrets' -> + New Client Secret -> set description and expirty -> Copy the secret Value -> copy and paste this as a variable in your notebook

- Step 3: Configure databricks:
In your notebook, add:
```
client_id = 'your_client_id'
tenant_id = 'your_tenant_id'
client_secret = 'your_client_secret'

spark.conf.set("fs.azure.account.auth.type.formula1lgdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1lgdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1lgdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1lgdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1lgdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
```

4. Assign Role 'Storage Blob Data Contributor to the Data Lake
from your storage account page, -> Access Control (IAM) -> Add -> Add role assignment -> search for 'Storage Blob Data Contributor', select and click next -> search for your apps name -> select -> Review + assign (twice)

And thats it. Your notebook is authenticated.


#### Authentication using Session/Cluster Scoped Authentication

The previous methods are examples of Session scoped authentication, as the authentication obtained to access the data is only valid within the session. Once we detach the notebook from the cluster, we will have to re-authenticate again in order to access the storage account from the notebook.  
Instead of authenticating from notebooks, can we authenticate from  within the cluster, so that every notebook connecting to the cluster has access to the storage. This is what is referred to as Cluster Scoped Authentication. We can do that by specifying the Spark configuration parameters and the corresponding secret values within the Cluster configuration. So when the Cluster is created, it already has the Spark configuration set with the right secret values. During the Cluster creation, the authentication with the Data Lake will be complete, i.e. Spark configurations are executed. One drawback with this approach, is that every notebook which runs on this cluster will have the access to the Data Lake. So this cluster could not be used for other purposes, where some of the users should have limited access to the data or no access to this data at all. Effectively, by defining the credentials in the cluster, we are making the cluster specific for one permission group, in which all users require the same level of access. This may not be what you want in a large project. For example, if the Data Engineers want permissions to certain storage accounts, but the Analysts should not have, you need to create two clusters for the specific purposes and they cannot share one cluster. Because of this,
it's not a widely used approach in the industry.  

#### Authentication using Azure Active Directory credential passthrough
Databricks will pass the users, Azure Active Directory credentials to the ADL storage account to authenticate. If the specific user has the required role assigned in RBAC for the storage account, they'll be able to access a storage account otherwise, they won't be able to access the storage account.
This is really useful in scenarios such as Multiple Machine Learning Engineers are using the same Databricks Cluster, but each one should have access to only specific storage accounts.
This option is only available for workspaces using premium tier.

From the databricks workspace -> compute -> select your cluster -> edit -> Advanced Options -> tick the 'Enable credential passthrough for user-level data access' -> confirm and restart
Then you need to create an IAM role:
Access Control -> + add -> add role assignment -> select the role with the level of permission you want -> add the user you want (not the app this time)


### Securing your credentials in Secrets
Databricks offers a service called Secret Scope, and Azure offers another service called Key Vault and we can combine the two to create a more secure solution in Azure Databricks.
Secret Scopes in Databricks, is basically a collection of secrets identified by a name. Secret Scopes helps store the credentials securely and reference them in notebooks, clusters, as well
as jobs when required.   
There are two types of secret scopes in Azure Databricks.   
The first one is the Databricks backed Secret Scope. In this case, a Secret Scope is backed by an encrypted databricks database owned and managed by Databricks. We can create and alter the encrypted database using the Databricks CLI or the API. It cannot be done via the Graphical User Interface.  
The other type of secret scope is the Azure Key Vault backed Secret Scope. In this case, the secrets are created and managed in Azure Key Vault. This is the recommended approach when using Databricks on Azure. This is mainly because by keeping the secrets in Azure Key Vault, the secrets can be shared amongst other Azure services such as Azure Data Factory, Azure Synapse Analytics, etc. And the Key Vault can be the one place where all the secrets are kept and maintained.  
In order to implement an Azure Key Vault backed solution, we need to create an Azure Key Vault and add all the secrets to the key vault. We then need to create the Databricks Secret Scope and link it to the Azure Key Vault. Now that the secret scope is linked to the key vault, we will be able to use notebooks, clusters or jobs, to get the secrets using the Databricks Secrets Utility called dbutils.secrets.


Steps:
1. create an Azure Key Vault
2. Add all the secrets to the key vault
3. create the Databricks Secret Scope and link it to the Azure Key Vault
4. In a notebook, get the secrets using the Databricks Secrets Utility called dbutils.secrets.



- Step 1: Create Azure Key Vault  
Create a resource -> search for Key Vault -> create -> select your resource group -> provide a name and select your region -> select your settings (default settings are acceptable here)  -> create and pin to dashboard 

- Step 2: Add all the secrets to the key vault
-> Secrets -> + Generate/Import -> select manual and provide name and secret value -> add you credentials

- Step 3: Create the Databricks Secret Scope and link it to the Azure Key Vault
from the databricks homepage, you need to access the secret scope by modifying the url (Screenshot) to add /secrets/createScope
-> provide a name for your secret scope -> select a Mange Principal from the drop down options. The options are Creator and All Users. Creator allows access to the secret scope only for the user who created it, and All users allows all users who has access to the workspace to have access to the secret scope as well. Creator is only available on premium tier (all users is fine for this example) ->  link this secret scope to Azure Key Vault, that we created previously, by providing a DNS name and a Resource ID. You can get those from your key vault -> Properties -> Vault URI is the DNS name (screenshot) and Resource ID is the Resource ID required in the secret scope. -> create


- Step 4: Use dbutils.secrets in your notebooks


sp=rl&st=2023-07-28T23:48:16Z&se=2023-07-29T07:48:16Z&spr=https&sv=2022-11-02&sr=c&sig=Y8J%2BEDYT0W5PyXGV31y5UOJ0ncQOwIG5I6Lrg5ioy2Q%3D

https://formula1lgdl.blob.core.windows.net/demo?sp=rl&st=2023-07-28T23:48:16Z&se=2023-07-29T07:48:16Z&spr=https&sv=2022-11-02&sr=c&sig=Y8J%2BEDYT0W5PyXGV31y5UOJ0ncQOwIG5I6Lrg5ioy2Q%3D


### Mounting Data Lake Containers to Databricks

#### Databricks File System (DBFS)

DBFS is simply a file system that provides distributed access to the data stored in Azure storage. It's not a storage solution in itself. The storage here is the Azure Blob Storage, and this is the default storage that's created when the Databricks workspace was deployed. This DBFS mount on the default Azure Blob Storage is called DBFS Root. DBFS Root is backed by Azure Blob Storage in the databricks created Resource Group. You can access one of the special folders within DBFS Root called File Store via the Web User Interface. You can use this as a temporary storage, for example, to store any images to be used in notebooks or some data to play with quickly.

Databricks also stores query results from commands such as display in DBFS Root. Similar to Hive, Databricks also allows us to create both managed and external tables. If you create a managed table without specifying the location for the database, that data will also be stored in DBFS Root, i.e. the default location for managed tables is DBFS Root. But you can change that during the database creation. Even though DBFS Root is the default storage for Databricks, it's not the recommended location to store customer data. When you drop the Databricks workspace, this storage also gets dropped, which is not what you would want for the customer data. Instead, we can use an external Data Lake, fully controlled by the customer and we can mount that to
the workspace.

We can view files using ```dbutils.fs.ls('/')```
```/results/``` is where Databricks keeps any temporary outputs during execution. For example, even the output from this display command is kept within the databricks-results folder.
There is also another special folder called FileStore to which we can upload files using the Databricks UI.

If you've recently created the workspace, the DBFS file browser UI which you can use to upload files to file store is not enabled by default. So let's enable that first. In order to enable that, you will have to go to admin console. So go over to the top right and select Admin Settings from dropdown and then go to the Workspace settings and search for DBFS. And as you can see at the moment, the DBFS File Browser is disabled. So in your workspace, if that is disabled, please click on that button here to enable the DBFS File Browser. And once you've done that, refresh the browser to ensure that the settings change that we applied takes effect. So now click on the Data menu from the sidebar. And as you can see, we've got a new button here called Browse DBFS and click on that.

Not, you cant restrict access to the file store, so everyone can view anything in the file store. I generally tend to use the FileStore folder in two scenarios. One is when I want to do some quick analysis on a set of data, which is very small volume and I don't want to worry about authentication. And also I keep files in this folder that I'm using in the notebooks as part of markdown language. This could be things like images, charts, etc.

#### Databricks Mount Overview  

we shouldn't be using DBFS Root for keeping customer data. Customers can create separate Azure Blob Storage or Azure Data Lake storage accounts in their subscription and keep the data in them. In this Architecture, when you delete the Databricks workspace, the customer data still stays without being untouched. In order to access the storage, we can use the ABFS protocol.

However, a better approach is to mount the storage accounts to DBFS directly. Once it's mounted, everyone who has access to the workspace can access the data without providing the credentials. Also, they will be able to use the file system semantics rather than the long URLs.  You can then access the mount points without specifying the credentials.

This was the recommended solution from Databricks to access Azure Data Lake until the introduction of Unity Catalog, which became generally available around end of 2022. Databricks now recommends using the Unity Catalog for better security, but most projects I see today are still using Databricks Mounts to access the data. , Unity Catalog provides centralized access when you have multiple databricks workspaces. So if you are on a large project with multiple Databricks workspaces, Unity Catalog serves you better.  

However, many project still use mounts so lets take a look at these. In order to create a mount, we need to create a Service Principal. A Service Principal is nothing but an Azure Active Directory credential, similar to your user account. You can consider this as a service account. Once we have created the Service Principal, we need to grant access for the Data Lake storage to the Service Principal. We can then create the mount points in DBFS using these credentials. The mount points we create provide access to the storage without requiring credentials.

Steps:

