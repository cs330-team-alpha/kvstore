import string
import random
import os.path

# Sample stream for key-value store application.
# Output csv format: [timestamp (in ms)], [request type], [data key], [data value]
# Assuming the data base has N = 1000 existing entries

def random_request(t):
    types = ['fetch', 'fetch', 'fetch', 'fetch', 'fetch',
             'fetch', 'fetch', 'fetch', 'fetch', 'fetch', 'update']
    chars = string.ascii_letters + string.digits
    r = random.choice(types)
    k = random.randrange(1, 1000)
    v = ''.join(random.choice(chars) for x in xrange(6))
    line = "%d, %s, %d%s\n" % (t, r, k ,', '+ v if (r == 'update') else '')
    return line

def generate(num = 1):
    new_file = 'requests/user_requests_r10w1.csv'
    if (os.path.isfile(new_file)):
        print "Error: '%s' already exists." % new_file
    else:
        t = 0
        content = "Time, Request, Key, Value\n"
        for i in xrange(num):
            content += random_request(t)
            t += random.randrange(100, 1000)
        with open(new_file, 'w') as f:
            f.write(content)
        print "Success: '%s' generated!" % new_file
generate(100)


