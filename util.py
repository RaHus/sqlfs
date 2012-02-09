#from IPython.Shell import IPShellEmbed
#args = ['-pdb', '-pi1', 'In <\\#>: ', '-pi2', '   .\\D.: ',
#    '-po', 'Out<\\#>: ', '-nosep']
#dbg = IPShellEmbed(args,
#    banner = 'Entering IPython.  Press Ctrl-D to exit.',
#    exit_msg = 'Leaving Interpreter, back to Pylons.')
def dbg(*args,**kwargs):
    pass

def dirFromList(list):
    """
    Return a properly formatted list of items suitable to a directory listing.
    [['a', 'b', 'c']] => [[('a', 0), ('b', 0), ('c', 0)]]
    """
    mylist = [('.', 0), ('..', 0)]
    return [mylist.extend([(x, 0) for x in list])]

def getDepth(path):
    """
    Return the depth of a given path, zero-based from root ('/')
    """
    if path == '/':
        return 0
    else:
        return path.count('/')

def getParts(path):
    """
    Return the slash-separated parts of a given path as a list
    """
    if path in '/':
        return [['/']]
    else:
        return path.split('/')


