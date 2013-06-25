import os
import mmap
import time
import json

MAX_SIZE=mmap.PAGESIZE

FILE='/dev/sdb1'

MAX_ENTRIES = 32

t0 = time.time()

for J in range(MAX_ENTRIES,0, -1):
    print J
    d = {'name' : 'Arbiter-master'+str(J), 'last_alive' : int(time.time()), 'host_name':'sgemini', 'id':J}
    
    s = json.dumps(d)
    
    print "WILL HAVE TO SAVE", s, "len", len(s)

    if not os.path.exists(FILE):
        print "TRY TO CREATE A DUMMY %s file of %s size" % (FILE, MAX_ENTRIES*MAX_SIZE)
        fd = os.open(FILE, os.O_CREAT|os.O_WRONLY)
        os.write(fd, '\0'*MAX_ENTRIES*MAX_SIZE)
        os.close(fd)
        print "FILE %s created" % FILE
    else:
        # Assume that the file is quite as big as it should
        f = os.open(FILE, os.O_RDWR)
        end = os.lseek(f, 0, os.SEEK_END)
        print "END IS AT", end
        if end < MAX_ENTRIES*MAX_SIZE:
            print "INCREAING THE QUORUM FILE to", MAX_ENTRIES*MAX_SIZE
            for p in range(end, MAX_ENTRIES*MAX_SIZE):
                os.write(f, '\0')
        os.close(f)
    
    fd = os.open(FILE, os.O_RDWR|os.O_DIRECT|os.O_SYNC)
    print "MAPPING", MAX_SIZE, "FROM", J*MAX_SIZE
    mm = mmap.mmap(fd, MAX_SIZE, offset=J*MAX_SIZE)
    # Jump to the arbiter entry J
    mm.seek(0)
    mm.write(s)
    # Add padding too
    mm.write('\0'* (MAX_SIZE - len(s)))
    #for i in range(len(s), MAX_SIZE):
    #    mm.write('\0')
    mm.close()
    # fsync should not be useful, but maybe...
    os.fsync(fd)
    os.close(fd)
    
    
    f = os.open(FILE, os.O_RDONLY)
    os.lseek(f, J*MAX_SIZE, os.SEEK_SET)
    line = os.read(f, MAX_SIZE)
    # Remove the padding
    line = line.split('\0')[0]
    print "LEN", len(line)
    print line
    d = json.loads(line)
    print "READ", d
    os.close(f)
    print time.time() - t0


