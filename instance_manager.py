import boto3
import paramiko
import time

class InstanceManager:
    def __init__(self, aws_config):
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.aws_config = aws_config
        
    def launch_instance(self, instance_type='t2.micro', name='Instance'):
        """Launch an EC2 instance and tag it with a name."""
        response = self.ec2_client.run_instances(
            ImageId=self.aws_config['image_id'],
            InstanceType=instance_type,
            KeyName=self.aws_config['key_name'],
            SecurityGroupIds=[self.aws_config['security_group_id']],
            MinCount=1,
            MaxCount=1
        )
        instance_id = response['Instances'][0]['InstanceId']
        
        waiter = self.ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        
        self.ec2_client.create_tags(
            Resources=[instance_id],
            Tags=[{'Key': 'Name', 'Value': name}]
        )
        
        ec2_resource = boto3.resource('ec2', region_name='us-east-1')
        instance = ec2_resource.Instance(instance_id)
        
        while instance.public_ip_address is None:
            time.sleep(5)
            instance.reload()
        
        print(f"Launched instance: {instance_id} with name: {name}")
        return instance

    def wait_for_system_ready(self, ssh):
        """Wait for system to be ready for package installation"""
        max_retries = 30
        retry_interval = 10
        
        for _ in range(max_retries):
            try:
                # Check if apt lock is released
                stdin, stdout, stderr = ssh.exec_command(
                    "lsof /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || echo 'free'"
                )
                result = stdout.read().decode().strip()
                
                if result == 'free':
                    return True
                    
            except Exception:
                pass
                
            time.sleep(retry_interval)
            
        return False

    def setup_instance(self, instance):
        """Setup Python environment on instance"""
        try:
            # Initial wait for instance to be ready
            time.sleep(90)
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(f"{self.aws_config['key_name']}.pem")
            
            # Try to connect with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(30)
            
            # Wait for system to be ready
            if not self.wait_for_system_ready(ssh):
                raise Exception("System not ready for package installation after maximum wait time")
            
            commands = [
                "sudo apt-get update -y",
                "sudo apt-get install -y python3-pip",
                "pip3 install boto3"
            ]
            
            for cmd in commands:
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error_output = stderr.read().decode()
                    raise Exception(f"Command '{cmd}' failed with status {exit_status}, error: {error_output}")
            
            ssh.close()
            print(f"Setup completed for instance: {instance.id}")
        except Exception as e:
            print(f"Error setting up instance {instance.id}: {e}")
            raise

    def deploy_code(self, instance, script_name):
        """Deploy code to instance"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(f"{self.aws_config['key_name']}.pem")
            ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
            
            sftp = ssh.open_sftp()
            sftp.put(script_name, f'/home/ubuntu/{script_name}')
            ssh.exec_command(f'chmod +x /home/ubuntu/{script_name}')
            
            sftp.close()
            ssh.close()
            print(f"Deployed {script_name} to instance: {instance.id}")
        except Exception as e:
            print(f"Error deploying code to instance {instance.id}: {e}")
            raise

    def run_ssh_command(self, instance, command):
        """Run command on remote instance"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(f"{self.aws_config['key_name']}.pem")
            ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
            
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error_output = stderr.read().decode()
                raise Exception(f"Command failed with status {exit_status}, error: {error_output}")
            
            ssh.close()
        except Exception as e:
            print(f"Error running command on instance {instance.id}: {e}")
            raise