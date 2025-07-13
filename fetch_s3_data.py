import os
import yaml
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3DataFetcher:
    def __init__(self, credentials_file="Monthly_report_AWS.yaml", config_file="Monthly_Report.yaml", output_dir="output"):
        """
        Initialize S3 data fetcher with configuration and output directory.
        
        Args:
            credentials_file (str): Path to AWS credentials YAML file
            config_file (str): Path to configuration YAML file containing bucket info
            output_dir (str): Directory to save downloaded files
        """
        self.credentials_file = credentials_file
        self.config_file = config_file
        self.output_dir = Path(output_dir)
        self.credentials = self._load_credentials()
        self.config = self._load_config()
        self.s3_client = self._create_s3_client()
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_credentials(self):
        """Load AWS credentials from YAML file."""
        try:
            with open(self.credentials_file, "r") as file:
                credentials = yaml.safe_load(file)
                logger.info(f"AWS credentials loaded from {self.credentials_file}")
                return credentials
        except FileNotFoundError:
            logger.error(f"Credentials file {self.credentials_file} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing credentials YAML file: {e}")
            raise
    
    def _load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, "r") as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self.config_file}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file {self.config_file} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration YAML file: {e}")
            raise
    
    def _create_s3_client(self):
        """Create and configure S3 client using credentials from credentials file."""
        try:
            # Try to use credentials from credentials file first
            if all(key in self.credentials for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']):
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.credentials['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=self.credentials['AWS_SECRET_ACCESS_KEY'],
                    region_name=self.credentials.get('AWS_REGION', 'us-east-1')
                )
                logger.info("S3 client created using credentials file")
            else:
                # Fall back to environment variables or IAM roles
                s3_client = boto3.client(
                    's3',
                    region_name=self.credentials.get('AWS_REGION', 'us-east-1')
                )
                logger.info("S3 client created using default credential chain")
            
            return s3_client
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please check your credentials file.")
            raise
        except Exception as e:
            logger.error(f"Error creating S3 client: {e}")
            raise
    
    def list_bucket_objects(self, bucket_name, prefix="", max_keys=1000):
        """
        List objects in the specified S3 bucket.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            prefix (str): Prefix to filter objects
            max_keys (int): Maximum number of objects to retrieve
            
        Returns:
            list: List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(objects)} objects in bucket '{bucket_name}' with prefix '{prefix}'")
                return objects
            else:
                logger.info(f"No objects found in bucket '{bucket_name}' with prefix '{prefix}'")
                return []
                
        except ClientError as e:
            logger.error(f"Error listing bucket objects: {e}")
            return []
    
    def download_file(self, bucket_name, object_key, local_file_path=None):
        """
        Download a single file from S3.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            object_key (str): Key of the object to download
            local_file_path (str): Local path to save the file (optional)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if local_file_path is None:
                # Create local file path based on object key
                local_file_path = self.output_dir / Path(object_key).name
            else:
                local_file_path = Path(local_file_path)
            
            # Create directory if it doesn't exist
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(bucket_name, object_key, str(local_file_path))
            logger.info(f"Downloaded '{object_key}' to '{local_file_path}'")
            return True
            
        except ClientError as e:
            logger.error(f"Error downloading '{object_key}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading '{object_key}': {e}")
            return False
    
    def download_all_files(self, bucket_name, prefix="", file_extensions=None):
        """
        Download all files from the specified S3 bucket.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            prefix (str): Prefix to filter objects
            file_extensions (list): List of file extensions to filter (e.g., ['.csv', '.json'])
            
        Returns:
            dict: Summary of download results
        """
        logger.info(f"Starting download from bucket '{bucket_name}' with prefix '{prefix}'")
        
        # Get list of objects
        objects = self.list_bucket_objects(bucket_name, prefix)
        
        # Filter by file extensions if specified
        if file_extensions:
            objects = [obj for obj in objects if any(obj.lower().endswith(ext.lower()) for ext in file_extensions)]
            logger.info(f"Filtered to {len(objects)} objects based on extensions: {file_extensions}")
        
        # Download files
        results = {
            'total_files': len(objects),
            'successful_downloads': 0,
            'failed_downloads': 0,
            'downloaded_files': [],
            'failed_files': []
        }
        
        for obj_key in objects:
            # Skip directories (objects ending with '/')
            if obj_key.endswith('/'):
                continue
                
            # Create local file path preserving S3 structure
            local_file_path = self.output_dir / obj_key
            
            if self.download_file(bucket_name, obj_key, local_file_path):
                results['successful_downloads'] += 1
                results['downloaded_files'].append(obj_key)
            else:
                results['failed_downloads'] += 1
                results['failed_files'].append(obj_key)
        
        logger.info(f"Download complete. Success: {results['successful_downloads']}, Failed: {results['failed_downloads']}")
        return results
    
    def fetch_bucket_data(self, bucket_name=None, prefix="", file_extensions=None):
        """
        Main method to fetch data from S3 bucket.
        
        Args:
            bucket_name (str): Name of the S3 bucket (uses config if not provided)
            prefix (str): Prefix to filter objects
            file_extensions (list): List of file extensions to filter
            
        Returns:
            dict: Summary of download results
        """
        # Use bucket name from config if not provided
        if bucket_name is None:
            bucket_name = self.config.get('S3_BUCKET', self.config.get('bucket'))
            if not bucket_name:
                logger.error("No S3 bucket specified in Monthly_Report.yaml config file or parameters")
                logger.error("Please add 'S3_BUCKET' or 'bucket_name' to your Monthly_Report.yaml file")
                return None
        
        logger.info(f"Fetching data from S3 bucket: {bucket_name}")
        logger.info(f"Output directory: {self.output_dir}")
        
        # Create timestamped subdirectory for this fetch
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_output = self.output_dir / f"fetch_{timestamp}"
        original_output = self.output_dir
        self.output_dir = timestamped_output
        
        try:
            results = self.download_all_files(bucket_name, prefix, file_extensions)
            results['output_directory'] = str(timestamped_output)
            return results
        finally:
            # Restore original output directory
            self.output_dir = original_output


def main():
    """Main function to demonstrate usage."""
    try:
        # Initialize the S3 data fetcher
        fetcher = S3DataFetcher()
        
        # Example: Fetch all CSV files from the bucket
        results = fetcher.fetch_bucket_data(
            prefix="",  # No prefix - fetch all
            file_extensions=['.csv', '.json', '.xlsx', '.txt']  # Common data file types
        )
        
        if results:
            print("\n=== Download Summary ===")
            print(f"Total files: {results['total_files']}")
            print(f"Successful downloads: {results['successful_downloads']}")
            print(f"Failed downloads: {results['failed_downloads']}")
            print(f"Output directory: {results['output_directory']}")
            
            if results['downloaded_files']:
                print("\nDownloaded files:")
                for file in results['downloaded_files'][:10]:  # Show first 10 files
                    print(f"  - {file}")
                if len(results['downloaded_files']) > 10:
                    print(f"  ... and {len(results['downloaded_files']) - 10} more files")
            
            if results['failed_files']:
                print("\nFailed downloads:")
                for file in results['failed_files']:
                    print(f"  - {file}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
