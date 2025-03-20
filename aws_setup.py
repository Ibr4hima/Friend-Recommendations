import boto3
import os
from botocore.exceptions import ClientError

class AWSResourceManager:
    def __init__(self, key_name='mapreduce-key', security_group_name='mapreduce-sg', bucket_name='mapreduce-socialnetwork'):
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.s3_client = boto3.client('s3')
        self.key_name = key_name
        self.security_group_name = security_group_name
        self.bucket_name = bucket_name
        
    def setup_aws_resources(self):
        """Setup required AWS resources"""
        self.create_key_pair()
        self.vpc_id = self.get_default_vpc_id()
        self.security_group_id = self.create_security_group()
        self.image_id = self.get_ubuntu_ami()
        self.create_s3_bucket()
        return {
            'key_name': self.key_name,
            'security_group_id': self.security_group_id,
            'image_id': self.image_id,
            'vpc_id': self.vpc_id
        }

    def create_key_pair(self):
        """Create an EC2 key pair"""
        try:
            self.ec2_client.delete_key_pair(KeyName=self.key_name)
        except ClientError:
            pass
        
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response['KeyMaterial']
        
        key_file = f"{self.key_name}.pem"
        with open(key_file, 'w') as f:
            f.write(private_key)
        os.chmod(key_file, 0o400)
        print(f"Created key pair: {self.key_name}")

    def get_default_vpc_id(self):
        """Get the default VPC ID"""
        response = self.ec2_client.describe_vpcs(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]
        )
        return response['Vpcs'][0]['VpcId']

    def create_security_group(self):
        """Create a security group for the instances"""
        try:
            response = self.ec2_client.create_security_group(
                GroupName=self.security_group_name,
                Description='Security group for MapReduce instances',
                VpcId=self.vpc_id
            )
            security_group_id = response['GroupId']
            
            self.ec2_client.authorize_security_group_ingress(
                GroupId=security_group_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 22,
                        'ToPort': 22,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                    }
                ]
            )
            print(f"Created security group: {self.security_group_name}")
            return security_group_id
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
                response = self.ec2_client.describe_security_groups(
                    Filters=[{'Name': 'group-name', 'Values': [self.security_group_name]}]
                )
                return response['SecurityGroups'][0]['GroupId']
            raise

    def get_ubuntu_ami(self):
        """Get latest Ubuntu AMI ID"""
        response = self.ec2_client.describe_images(
            Filters=[
                {'Name': 'name', 'Values': ['ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*']},
                {'Name': 'virtualization-type', 'Values': ['hvm']}
            ],
            Owners=['099720109477']
        )
        return sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]['ImageId']

    def create_s3_bucket(self):
        """Create S3 bucket for data storage"""
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            print(f"Created S3 bucket: {self.bucket_name}")
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise

    def cleanup_resources(self, instance_ids):
        """Cleanup AWS resources"""
        try:
            if instance_ids:
                self.ec2_client.terminate_instances(InstanceIds=instance_ids)
                print(f"Terminated instances: {instance_ids}")
            
            try:
                self.ec2_client.delete_key_pair(KeyName=self.key_name)
                os.remove(f"{self.key_name}.pem")
                print(f"Deleted key pair: {self.key_name}")
            except:
                pass
            
            try:
                self.ec2_client.delete_security_group(GroupId=self.security_group_id)
                print(f"Deleted security group: {self.security_group_name}")
            except:
                pass
            
        except Exception as e:
            print(f"Error in cleanup: {e}")