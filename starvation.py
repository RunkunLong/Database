import random, simpy

RANDOM_SEED = 30
TRTIME = 4.5      # Maximum amount of time it can take to process a transaction, will always take >= .5 and < 5
T_INTER = 2       # Create a new transaction every ~2 minutes
SIM_TIME = 10000     # Simulation time in minutes
datablockStates = [ ]   # Keeps track of the locks on the 100 different datablocks
process_queue = [ ] # Keeps track of how old processes are, in order to prevent starvation


class DatablockState(object):
    def __init__(self, DataItemID, lock_mode, number_of_readers):
        self.DataItemID = DataItemID
        self.lock_mode = lock_mode
        self.number_of_readers = number_of_readers

class Transaction(object):
   
    def __init__(self, env, capacity, trtime):
        self.env=env
        self.machine = simpy.Resource(env, capacity)
        self.trtime = trtime

    def processTr(self, name, l_mode, data_value):
        current_index = process_queue.index(name)
        while current_index > 4: #This is to check to make sure that starvation is not occuring, as the older transactions are processed before the newer ones
            current_index = process_queue.index(name)
            yield self.env.timeout(.25)
            
        if datablockStates[data_value].lock_mode == '':
            datablockStates[data_value].lock_mode = l_mode
            if (l_mode == 'read'):
                datablockStates[data_value].number_of_readers += 1
        elif datablockStates[data_value].lock_mode == 'read':
            if (l_mode == 'read'):
                datablockStates[data_value].number_of_readers += 1
            else:
                while datablockStates[data_value].number_of_readers > 0:
                    yield self.env.timeout(.25)
                datablockStates[data_value].lock_mode = 'write'
        else:
            while datablockStates[data_value].lock_mode == 'write':
                yield self.env.timeout(.25)
            datablockStates[data_value].lock_mode = l_mode
            if l_mode == 'read':
                datablockStates[data_value].number_of_readers += 1
        print('%s obtained a %s lock on %d at %.2f' % (name, l_mode, data_value, env.now))
        processTime = random.random() * TRTIME + .5
        yield self.env.timeout(processTime)
        if (l_mode == 'write'):
            datablockStates[data_value].lock_mode = ''
        else:
            datablockStates[data_value].number_of_readers -= 1
            if datablockStates[data_value].number_of_readers == 0:
                datablockStates[data_value].lock_mode = ''
        print('%s finished the %s operation on datablock %d at time %.2f.' % (name, l_mode, data_value, env.now))
        process_queue.remove(name)


def request(env, name, tr, lock_mode, data_value):
    print('%s is being processed at %.2f, with a request for a %s on datablock %d.' % (name, env.now, lock_mode, data_value))
    with tr.machine.request() as request:
        yield request
        
        yield env.process(tr.processTr(name, lock_mode, data_value))


def setup(env, capacity, trtime, t_inter):
    transaction = Transaction(env, capacity, trtime)

    # Create 5 initial transactions
    for i in range(5):
        data_value = random.randint(0, 100)
        read_or_write = random.random()
        lock_mode = ""
        if (read_or_write < .2):
            lock_mode = "write"
        else:
            lock_mode = "read"
        process_queue.append('T %d' % i)
        env.process(request(env, 'T %d' % i, transaction, lock_mode, data_value))

    # Create more transactions while the simulation is running
    while True:
        yield env.timeout(random.randint(t_inter-2, t_inter+2))
        i += 1
        data_value = random.randint(0, 100)
        read_or_write = random.random()
        lock_mode = ""
        if (read_or_write < .2):
            lock_mode = "write"
        else:
            lock_mode = "read"
        process_queue.append('T %d' % i)
        env.process(request(env, 'T %d' % i, transaction, lock_mode, data_value))

for i in range(0, 101):
    datablockStates.append(DatablockState(i, '', 0))


# Setup and start the simulation
print('Read and Write transactions')
random.seed(RANDOM_SEED)

# Create an environment and start the setup process
env = simpy.Environment()
env.process(setup(env, 5, TRTIME, T_INTER))

# Execute!
env.run(until=SIM_TIME)