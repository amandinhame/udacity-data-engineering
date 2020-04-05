import configparser
import boto3

def delete_redshift_cluster(config):
    """Deletes the Redshift cluster created using the configuration in config.
    
    Args:
        config: Configuration file parser. 
    """
    
    try:
        print('Deleting redshift cluster')

        # Clients
        redshift = boto3.client('redshift', 
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
            region_name=config.get('AWS', 'REGION'))
        
        redshift.delete_cluster(ClusterIdentifier=config.get('CLUSTER', 'IDENTIFIER'), SkipFinalClusterSnapshot=True)
        
        print('Finished deleting redshift cluster')
    except Exception as e:
        print(e)

def main():
    """Deletes cluster without snapshot."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    delete_redshift_cluster(config)
    

if __name__ == "__main__":
    main()