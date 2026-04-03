import math

if __name__ == '__main__':
    total_entities = 100000

    micro_batch_size = 100

    num_partitions = max(1, math.ceil(total_entities / micro_batch_size))

    print('Number of partoitions: {}'.format(num_partitions))