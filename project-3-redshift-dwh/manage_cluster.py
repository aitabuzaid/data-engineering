import configparser
import boto3
import json
import pandas as pd

def prettyRedshiftProps(props):
    """Prints the properties of the redshift cluster in a visually appealing table.
    
    Keyword arguments:
    props -- a dictionary of the props of the redshift cluster.
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def main():
    """This program manages creating and deleting a redshift cluster through 
    infrastructure as code (IAC). The program has three options:
    1 - to create the cluster
    2 - to checke the status of the cluster
    3 - to delete the cluster
    """
    ## load the configuration file and variables
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    # create AWS resources: ec2, s3, iam, and redshift 
    ec2 = boto3.resource('ec2',
                    region_name = "us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3',
                   region_name = "us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                   region_name = "us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift',
                   region_name = "us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)
    
    option = input("Please choose an option:\n"+
                   "Enter '1' to create the redhshift cluster\n"+
                   "Enter '2' to check the status of the redshift cluster\n"+
                   "Enter '3' to delete the redshift cluster\n"+
                   "Enter an option:  ")
    

    if (option == '1'):  
        # create the redshift cluster
        try:
            print('1.1 Creating a new IAM Role')
            dwhRole = iam.create_role(
                Path = '/',
                RoleName = DWH_IAM_ROLE_NAME,
                Description = "Allows Refshift clusters to call AWS services",
                AssumeRolePolicyDocument = json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow','Principal': {'Service':'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'}))
        except Exception as e:
            print(e)
        
            print('1.2 Attaching Policy')
            iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                              )['ResponseMetadata']['HTTPStatusCode']
    
            print('1.3 Get the IAM role ARN')
            roleArn = iam.get_role(RoleName = DWH_IAM_ROLE_NAME)['Role']['Arn']
            print(roleArn)

        try:
            response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE, 
            NumberOfNodes = int(DWH_NUM_NODES),

            # add parameters for identifiers & credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
        
            # add parameter for role (to allow s3 access)
            IamRoles = [roleArn])
        except Exception as e:
            print(e)
        
    
    if (option == '2'):
        # check the status of the redshift cluster
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            print(prettyRedshiftProps(myClusterProps)) 
        except Exception as e:
            print(e)
        if (myClusterProps['ClusterStatus']=='available'):
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
            print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        
        

    if (option == '3'):
        # delete the cluster and the IAM role
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    
        
    
    
if __name__ == "__main__":
    main()