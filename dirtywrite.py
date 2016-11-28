import random, simpy
#from future import division

RANDOM_SEED = 42
BLOCK_NUM = 25    # Number of datablock
NUM_MACHINES = 4  # Number of readers and writers available
TRTIME = 4.5      # Maximum amount of time it can take to process a transaction, will always take >= .5 and < 5
T_INTER = 2       # Create a new transaction every ~2 minutes
SIM_TIME = 10000     # Simulation time in minutes
datablockStates = [ ]   # Keeps track of the locks on the 100 different datablocks
process_queue = [ ] # Keeps track of how old processes are, in order to prevent starvation
RUN_TIME = 10   #Total running time

global writenum
global invalidwritenum
writenum = 0
invalidwritenum = 0

class DatablockState(object):
    def __init__(self, DataItemID, lock_mode, number_of_readers,timest):
        self.DataItemID = DataItemID
        self.lock_mode = lock_mode
        self.number_of_readers = number_of_readers
        self.timestamp = timest

class Transaction(object):

    #global writenum
    #global invalidwritenum
   
    def __init__(self, env, num_machines, trtime):
        self.env = env
        self.machine = simpy.Resource(env, num_machines)
        self.trtime = trtime
        #self.trtimestamp = trst

    def processTr(self, name, l_mode, data_value):

        global writenum
        global invalidwritenum

        current_index = process_queue.index(name)
        while current_index > 4: #This is to check to make sure that starvation is not occuring, as the older transactions are processed before the newer ones
            current_index = process_queue.index(name)
            yield self.env.timeout(.25)
            

        if(l_mode == 'read'):
            #trtimestamp = datablockStates[data_value].readtimestamp + 1
            processTime = random.random() * TRTIME + .5
            yield self.env.timeout(processTime)
            datablockStates[data_value].timestamp = datablockStates[data_value].timestamp + 1

        if(l_mode == 'write'):
            writenum = writenum + 1
            trtimestamp = datablockStates[data_value].timestamp + 1
            processTime = random.random() * TRTIME + .5
            yield self.env.timeout(processTime)
            while trtimestamp <= datablockStates[data_value].timestamp:
                trtimestamp = datablockStates[data_value].timestamp + 1
                processTime = random.random() * TRTIME + .5
                yield self.env.timeout(processTime)
                print('%s face with the invalid dirty write on datablock %d at time %.2f, reattempted.' % (name, data_value, env.now))
                invalidwritenum = invalidwritenum + 1
            if(trtimestamp > datablockStates[data_value].timestamp):
                datablockStates[data_value].writetimestamp = trtimestamp
        
        print('%s finished the %s operation on datablock %d at time %.2f.' % (name, l_mode, data_value, env.now))
        process_queue.remove(name)


def request(env, name, tr, lock_mode, data_value):
    print('%s is being processed at %.2f, with a request for a %s on datablock %d.' % (name, env.now, lock_mode, data_value))
    with tr.machine.request() as request:
        yield request
        
        yield env.process(tr.processTr(name, lock_mode, data_value))


def setup(env, num_machines, trtime, t_inter):
    transaction = Transaction(env, num_machines, trtime)

    # Create 6 initial transactions
    for i in range(6):
        data_value = random.randint(0, BLOCK_NUM)
        read_or_write = random.random()
        lock_mode = ""
        if (read_or_write < .25):
            lock_mode = "write"
        else:
            lock_mode = "read"
        process_queue.append('T %d' % i)
        env.process(request(env, 'T %d' % i, transaction, lock_mode, data_value))

    # Create more transactions while the simulation is running
    while True:
        yield env.timeout(random.randint(t_inter-2, t_inter+2))
        i += 1
        data_value = random.randint(0, BLOCK_NUM)
        read_or_write = random.random()
        lock_mode = ""
        if (read_or_write < .25):
            lock_mode = "write"
        else:
            lock_mode = "read"
        process_queue.append('T %d' % i)
        env.process(request(env, 'T %d' % i, transaction, lock_mode, data_value))

for i in range(0, BLOCK_NUM+1):
    datablockStates.append(DatablockState(i, '', 0,0))


# Setup and start the simulation
print('Read and Write transactions')
random.seed(RANDOM_SEED)

# Create an environment and start the setup process
env = simpy.Environment()
env.process(setup(env, NUM_MACHINES, TRTIME, T_INTER))

# Execute!
i = RUN_TIME
while i >= 0:
    env.run(until=SIM_TIME)
    SIM_TIME += 10000
    i -= 1
percent = (invalidwritenum/writenum)*100
#print(percent)
print('When we run %d times, total write number: %d, invalid write number: %d. The percent of invalid write is %.3f %%.' %(RUN_TIME,writenum,invalidwritenum,percent))