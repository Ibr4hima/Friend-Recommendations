from aws_setup import AWSResourceManager
from instance_manager import InstanceManager
from data_processor import DataProcessor

class MapReduceOrchestrator:
    def __init__(self, input_file, n_mappers=3, n_reducers=2):
        self.input_file = input_file
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.mapper_instances = []
        self.reducer_instances = []
        
        # Initialize AWS resources
        self.aws_manager = AWSResourceManager()
        self.aws_config = self.aws_manager.setup_aws_resources()
        self.instance_manager = InstanceManager(self.aws_config)
        self.data_processor = DataProcessor(self.aws_config['key_name'])

    def run_mapreduce(self):
        """Execute MapReduce job"""
        try:
            # Launch and setup mapper instances
            print("Launching mapper instances...")
            for i in range(self.n_mappers):
                instance_name = f"Mapper {i+1}"
                instance = self.instance_manager.launch_instance(name=instance_name)
                self.instance_manager.setup_instance(instance)
                self.instance_manager.deploy_code(instance, 'mapper.py')
                self.mapper_instances.append(instance)
            
            # Launch and setup reducer instances
            print("Launching reducer instances...")
            for i in range(self.n_reducers):
                instance_name = f"Reducer {i+1}"
                instance = self.instance_manager.launch_instance(name=instance_name)
                self.instance_manager.setup_instance(instance)
                self.instance_manager.deploy_code(instance, 'reducer.py')
                self.reducer_instances.append(instance)
            
            # Split and distribute input data
            splits = self.data_processor.split_input_file(self.input_file, self.n_mappers)
            for i, instance in enumerate(self.mapper_instances):
                split_file = f'split_{i}.txt'
                with open(split_file, 'w') as f:
                    f.writelines(splits[i])
                self.instance_manager.deploy_code(instance, split_file)
            
            # Run mappers
            print("Running mappers...")
            for i, instance in enumerate(self.mapper_instances):
                self.instance_manager.run_ssh_command(
                    instance,
                    f'python3 /home/ubuntu/mapper.py < /home/ubuntu/split_{i}.txt > /home/ubuntu/mapper_output_{i}.txt'
                )
            
            # Collect and process mapper outputs
            print("Collecting mapper outputs...")
            all_mapper_outputs = self.data_processor.collect_mapper_outputs(self.mapper_instances)
            
            print("Partitioning mapper outputs...")
            partitions = self.data_processor.partition_mapper_outputs(all_mapper_outputs, self.n_reducers)
            
            # Distribute to reducers
            print("Distributing data to reducers...")
            self.data_processor.distribute_to_reducers(partitions, self.reducer_instances)
            
            # Run reducers
            print("Running reducers...")
            for i, instance in enumerate(self.reducer_instances):
                self.instance_manager.run_ssh_command(
                    instance,
                    f'python3 /home/ubuntu/reducer.py < /home/ubuntu/reducer_input_{i}.txt > /home/ubuntu/reducer_output_{i}.txt'
                )
            
            # Collect and process final results
            print("Collecting and processing final results...")
            self.data_processor.collect_and_process_results(self.reducer_instances)

        except Exception as e:
            print(f"Error in MapReduce job: {e}")
            raise

    def cleanup(self):
        """Cleanup all AWS resources"""
        instance_ids = [i.id for i in self.mapper_instances + self.reducer_instances]
        self.aws_manager.cleanup_resources(instance_ids)

def main():
    input_file = 'soc-LiveJournal1Adj.txt'  # Your input file
    n_mappers = 3
    n_reducers = 2
    
    orchestrator = MapReduceOrchestrator(input_file, n_mappers, n_reducers)
    
    try:
        orchestrator.run_mapreduce()
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Ask user about cleanup
        delete = input("Do you want to terminate all instances and cleanup resources? (yes/no): ").strip().lower()
        if delete == 'yes':
            orchestrator.cleanup()
        else:
            print("Instances and resources are left running.")

if __name__ == "__main__":
    main()