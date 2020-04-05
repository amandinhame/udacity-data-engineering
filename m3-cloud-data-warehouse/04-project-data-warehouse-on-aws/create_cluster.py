import configparser
import boto3
import time
import json


def create_redshift_iam_role(config):
    """Creates a IAM role to be used with Redshift using the configuration in config.
    
    Args:
        config: Configuration file parser.
    """
    
    try:
        print("Creating redshift role")

        # Clients
        iam = boto3.client('iam', 
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
            region_name=config.get('AWS', 'REGION'))

        # Create redshift role
        role_name = config.get('CLUSTER', 'IAM_ROLE_NAME')
        dwh_role = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description='Allows Redshift clusters to call AWS services',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

        # Attach S3 read-only policy
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')

        print("Finished creating redshift role")
    except Exception as e:
        print(e)
        

def create_redshift_cluster(config):
    """Creates a Redshift cluster using the configuration in config.
    
    Args:
        config: Configuration file parser.
    """
    
    try:
        print('Creating redshift cluster')

        # Clients
        redshift = boto3.client('redshift', 
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
            region_name=config.get('AWS', 'REGION'))
        
        iam = boto3.client('iam', 
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
            region_name=config.get('AWS', 'REGION'))

        # Create cluster
        redshift.create_cluster(        
            ClusterType=config.get('CLUSTER', 'TYPE'),
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes=int(config.get('CLUSTER', 'NUM_NODES')),
            ClusterIdentifier=config.get('CLUSTER', 'IDENTIFIER'),
            DBName=config.get('CLUSTER', 'DB_NAME'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            IamRoles=[iam.get_role(RoleName=config.get('CLUSTER', 'IAM_ROLE_NAME'))['Role']['Arn']])
        
        print('Finished creating redshift cluster')
    except Exception as e:
        print(e)
    
    
def wait_cluster(config):
    """Wait until the cluster is available.
    
    Args:
        config: Configuration file parser.
    """
    
    try:
        print('Waiting for cluster to be available.')
        
        # Check status
        status = ''
        while status != 'available':
            print('.', end='')
            status = get_cluster_info(config)['ClusterStatus']
            time.sleep(5)
        
        print('\nCluster available')
    except Exception as e:
        print(e)
        

def authorize_ingress(config):
    """Configure the security group of the cluster to allow ingress in the cluster configured port and all ip (should alter this to the specific ip of the machine where the process is running).
    
    Args:
        config: Configuration file parser.
    """
    
    try:
        print('Authorizing ingress')
        
        #Clients
        ec2 = boto3.resource('ec2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
            region_name=config.get('AWS', 'REGION'))

        # Vpc and authorize
        vpc = ec2.Vpc(id=get_cluster_info(config)['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.get('CLUSTER', 'DB_PORT')),
            ToPort=int(config.get('CLUSTER', 'DB_PORT')))
        
        print('Finished authorizing ingress')
    except Exception as e:
        print(e)
    
        
def get_cluster_info(config):
    """Return information about the cluster configured in config.
    
    Args:
        config: Configuration file parser.
        
    Returns:
        Description of the cluster in JSON format.
    """
    
    redshift = boto3.client('redshift', 
        aws_access_key_id=config.get('AWS', 'KEY'),
        aws_secret_access_key=config.get('AWS', 'SECRET'),
        region_name=config.get('AWS', 'REGION'))

    return redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]

def get_iam_role_info(config):
    """Return information about the iam role used by redshift.
    
    Args:
        config: Configuration file parser.
        
    Returns:
        Arn of the IAM role.
    """
    
    iam = boto3.client('iam', 
        aws_access_key_id=config.get('AWS', 'KEY'),
        aws_secret_access_key=config.get('AWS', 'SECRET'),
        region_name=config.get('AWS', 'REGION'))

    return iam.get_role(RoleName=config.get('CLUSTER', 'IAM_ROLE_NAME'))['Role']['Arn']
    
    
def main():
    """Creates and configure a cluster in AWS."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    create_redshift_iam_role(config)
    create_redshift_cluster(config)
    wait_cluster(config)
    authorize_ingress(config)
    

if __name__ == "__main__":
    main()