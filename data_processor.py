import paramiko
from collections import defaultdict

class DataProcessor:
    def __init__(self, key_name):
        self.key_name = key_name

    def split_input_file(self, input_file, n_mappers):
        """Split input file for mappers"""
        with open(input_file, 'r') as f:
            lines = f.readlines()
        
        lines_per_split = len(lines) // n_mappers + 1
        splits = []
        
        for i in range(0, len(lines), lines_per_split):
            splits.append(lines[i:i + lines_per_split])
        
        return splits

    def collect_mapper_outputs(self, mapper_instances):
        """Collect mapper outputs from mapper instances."""
        all_mapper_outputs = []
        for i, instance in enumerate(mapper_instances):
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(f"{self.key_name}.pem")
            ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
            
            sftp = ssh.open_sftp()
            with sftp.file(f'/home/ubuntu/mapper_output_{i}.txt', 'r') as f:
                mapper_output = f.readlines()
                all_mapper_outputs.extend(mapper_output)
            
            sftp.close()
            ssh.close()
        return all_mapper_outputs

    def partition_mapper_outputs(self, all_mapper_outputs, n_reducers):
        """Partition mapper outputs among reducers."""
        partitions = [[] for _ in range(n_reducers)]
        for line in all_mapper_outputs:
            parts = line.strip().split('\t')
            if len(parts) < 2:
                continue
            key = '\t'.join(parts[:2])
            reducer_index = hash(key) % n_reducers
            partitions[reducer_index].append(line)
        
        for i in range(len(partitions)):
            partitions[i].sort()
        return partitions

    def distribute_to_reducers(self, partitions, reducer_instances):
        """Distribute partitioned data to reducer instances."""
        for i, instance in enumerate(reducer_instances):
            partition_file = f'reducer_input_{i}.txt'
            with open(partition_file, 'w') as f:
                f.writelines(partitions[i])
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(f"{self.key_name}.pem")
            ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
            
            sftp = ssh.open_sftp()
            sftp.put(partition_file, f'/home/ubuntu/reducer_input_{i}.txt')
            ssh.exec_command(f'chmod +x /home/ubuntu/reducer_input_{i}.txt')
            
            sftp.close()
            ssh.close()

    def collect_and_process_results(self, reducer_instances):
        """Collect and process final results from reducers."""
        try:
            # Target users we want to process
            target_users = ['924', '8941', '8942', '9019', '9020', '9021', '9022', '9990', '9992', '9993']
            
            # Use defaultdict to combine recommendations from different reducers
            combined_recommendations = defaultdict(lambda: defaultdict(int))
            
            # Collect results from all reducers and combine them
            for i, instance in enumerate(reducer_instances):
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                key = paramiko.RSAKey.from_private_key_file(f"{self.key_name}.pem")
                ssh.connect(hostname=instance.public_ip_address, username='ubuntu', pkey=key)
                
                sftp = ssh.open_sftp()
                with sftp.file(f'/home/ubuntu/reducer_output_{i}.txt', 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        parts = line.split('\t')
                        if len(parts) != 2:
                            continue
                        
                        user_id, recommendations = parts
                        
                        # Only process target users
                        if user_id not in target_users:
                            continue
                        
                        # Process each recommendation and its count
                        for rec in recommendations.split(','):
                            if ':' in rec:
                                rec_id, count = rec.split(':')
                                count = int(count)
                                # Keep the highest count for each recommendation
                                current_count = combined_recommendations[user_id][rec_id]
                                combined_recommendations[user_id][rec_id] = max(current_count, count)
                
                sftp.close()
                ssh.close()
            
            # Write final results
            with open('final_recommendations.txt', 'w') as f:
                for user_id in target_users:
                    if user_id in combined_recommendations:
                        # Sort recommendations:
                        # 1. Primary sort by count (descending)
                        # 2. Secondary sort by recommendation ID (ascending) when counts are equal
                        sorted_recs = sorted(
                            combined_recommendations[user_id].items(),
                            key=lambda x: (-x[1], int(x[0]))
                        )
                        
                        # Extract just the IDs in the sorted order
                        rec_ids = [rec[0] for rec in sorted_recs]
                        
                        if rec_ids:  # Only write if there are recommendations
                            f.write(f"{user_id} {','.join(rec_ids)}\n")
            
            print("Results processed and written to final_recommendations.txt")
            
        except Exception as e:
            print(f"Error processing results: {e}")
            raise