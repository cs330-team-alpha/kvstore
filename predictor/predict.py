import getopt, sys
import urllib2

def pull_prediction(duration):
    # Pulling DrAFTS's prediction for us-west-2 m3.medium instance type        
    url = 'http://128.111.84.183/us-west-2-m3.medium.html'

    f = urllib2.urlopen(url)

    for line in f:
        if line.startswith('x: ['):
            x = [float(i) for i in line[4:-3].split(', ')]
        elif line.startswith('y: ['):
            y = [float(j) for j in line[4:-3].split(', ')]

    i = 0
    while (float(x[i]) < float(duration)):
        i += 1
    return y[i]

#def initial_setup(x, k=1):
    # Setup master node (balancer)



def usage():
    s = """
Usage: ./predict_price.py -d [time duration (in hours)] -b [budget limit (in dollars)]
    """
    print s

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:b:', ["help", "duration=", "budget="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ['-h', '--help']:
            usage()
            sys.exit()
        elif o in ['-d', '--duration']:
            duration = a
        elif o in ['-b', '--budget']:
            budget = a
        else: 
            assert False, "unhandled option"
    pred = pull_prediction(duration)
    print str(pred)


#if __name__ == '__main__':
#    main()
