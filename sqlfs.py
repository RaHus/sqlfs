#!bin/python

from fuse import Fuse
import fuse 
fuse.fuse_python_api = (0, 2)
import stat

from time import time

import stat    # for file properties
import os      # for filesystem modes (O_RDONLY, etc)
import errno   # for error number codes (ENOENT, etc)
               # - note: these must be returned as negatives



#from IPython.Shell import IPShellEmbed
#args = ['-pdb', '-pi1', 'In <\\#>: ', '-pi2', '   .\\D.: ',
#    '-po', 'Out<\\#>: ', '-nosep']
#dbg = IPShellEmbed(args,
#    banner = 'Entering IPython.  Press Ctrl-D to exit.',
#    exit_msg = 'Leaving Interpreter, back to Pylons.')


from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select#, update
from sqlalchemy.sql.expression import update

#from beaker.cache import CacheManager
#from beaker.util import parse_cache_config_options


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


class MyStatVfs(fuse.StatVfs):
    def __init__(self):
        self.f_blocks = 0
        self.f_bsize = 0
        self.f_favail = 0
        self.f_ffree = 0
        self.f_files = 0
        self.f_flag = 0
        self.f_frsize = 0
        self.f_namemax = 0 
        self.f_bavail = 0          
        self.f_bfree = 0           


class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class SQLFS(Fuse):
    """
    Sql to filesystem mapper. 
    """
    engine = None
    insp = None
    meta = None

    cache_opts = {
        'cache.type': 'memory',
        'cache.data_dir': '/tmp/cache/data',
        'cache.lock_dir': '/tmp/cache/lock'
    }
   # c = CacheManager(**parse_cache_config_options(cache_opts))

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)

        self.engine = create_engine('mysql://root:1ikko0ll@localhost/')
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.insp = Inspector.from_engine(self.engine)
        print 'Init complete.'


#    @c.cache('row', expire=600)
    def get_filerow(self, path):
        pe = getParts(path)
        table = Table(pe[-3], self.meta, schema=pe[-4], autoload=True)
        a=pe[-1].split('#_')
        ret = self.engine.execute(select(['*'],table.c[str(a[0])]==a[1])).fetchone()            
        return ', '.join(str(e) for e in ret)+"\n"

#    @c.cache('definition', expire=600)
    def get_definition(self, path):
        pe = getParts(path)
        table = Table(pe[-2], self.meta, schema=pe[-3], autoload=True)
        return str(CreateTable(table).compile(self.engine))



    def getattr(self, path):
        """
        - st_mode (protection bits)
        - st_ino (inode number)
        - st_dev (device)
        - st_nlink (number of hard links)
        - st_uid (user ID of owner)
        - st_gid (group ID of owner)
        - st_size (size of file, in bytes)
        - st_atime (time of most recent access)
        - st_mtime (time of most recent content modification)
        - st_ctime (platform dependent; time of most recent metadata change on Unix,
                    or the time of creation on Windows).
        """
        print 'getattr: ', path

        pe = path.split('/')[1:]
        st = MyStat()
        st.st_atime = int(time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime
        if path == '/':
            pass
        elif getDepth(path) == 1:
            pass
        elif getDepth(path) == 3 and ( pe[-1] == 'data' ):
            pass
        elif getDepth(path) == 4 and ( pe[-2] == 'data' ):
            st.st_mode = stat.S_IFREG | 0666
            st.st_nlink = 1
            st.st_size = len(self.get_filerow(path))  
        elif getDepth(path) == 3 and ( pe[-1] == 'definition' ):
            st.st_mode = stat.S_IFREG | 0666
            st.st_nlink = 1
            st.st_size = len(self.get_definition(path))
        return st

    def readdir(self, path, offset):
        """
        return: [[('file1', 0), ('file2', 0), ... ]]
        """

        pe = path.split('/')[1:]
        dirents = [ '.', '..' ]
        print '#########readdir: ',path,offset,pe[-1]
        if path == '/':
            dirents.extend(self.insp.get_schema_names())
        elif getDepth(path) == 1:
            dirents.extend([str(entry) for entry in self.insp.get_table_names(schema=pe[-1])] )
        elif getDepth(path) == 2:
            dirents.extend([ 'definition', 'data' ])
        elif getDepth(path) == 3 and pe[-1]=='data':
            table = Table(pe[-2], self.meta, schema=pe[-3], autoload=True)        
            ret = table.select().execute().fetchall()
            prefix = ''
            #dbg()
            for e in table.columns.keys():
                prefix += str(e)
                break     
            dirents.extend([prefix+'#_'+str(e[0]) for e in ret] )
        for r in dirents:
            yield fuse.Direntry(r)


    def read ( self, path, length, offset ):
        print '*** read', path, length, offset
        if getDepth(path) >= 2:
            pe = getParts(path)
        else:
            return -errno.ENOSYS 
        
        if getDepth(path) == 4 and ( pe[-2] == 'data' ):   
            a=self.get_filerow(path)
        elif getDepth(path) == 3 and ( pe[-1] == 'definition' ):
            a=self.get_definition(path)
        return a[offset:offset+length]

    def statfs ( self ):
        st = MyStatVfs()
        print '*** statfs'
        return st #-errno.ENOSYS

    def mythread ( self ):
        print '*** mythread'
        return -errno.ENOSYS

    def chmod ( self, path, mode ):
        print '*** chmod', path, oct(mode)
        return -errno.ENOSYS

    def chown ( self, path, uid, gid ):
        print '*** chown', path, uid, gid
        return -errno.ENOSYS

    def fsync ( self, path, isFsyncFile ):
        print '*** fsync', path, isFsyncFile
        return -errno.ENOSYS

    def link ( self, targetPath, linkPath ):
        print '*** link', targetPath, linkPath
        return -errno.ENOSYS

    def mkdir ( self, path, mode ):
        print '*** mkdir', path, oct(mode)
        return -errno.ENOSYS

    def mknod ( self, path, mode, dev ):
        print '*** mknod', path, oct(mode), dev
        return -errno.ENOSYS

    def open ( self, path, flags ):
        print '*** open', path, flags
        #return -errno.ENOSYS

    def readlink ( self, path ):
        print '*** readlink', path
        return -errno.ENOSYS

    def release ( self, path, flags ):
        print '*** release', path, flags
        return -errno.ENOSYS

    def rename ( self, oldPath, newPath ):
        print '*** rename', oldPath, newPath
        return -errno.ENOSYS

    def rmdir ( self, path ):
        print '*** rmdir', path
        return -errno.ENOSYS

    def symlink ( self, targetPath, linkPath ):
        print '*** symlink', targetPath, linkPath
        return -errno.ENOSYS

    def truncate ( self, path, size ):
        print '*** truncate', path, size
        #return -errno.ENOSYS
        pass    
    def unlink ( self, path ):
        print '*** unlink', path
        return -errno.ENOSYS

    def utime ( self, path, times ):
        print '*** utime', path, times
        return -errno.ENOSYS


    def write ( self, path, buf, offset ):
        print '*** write', path, buf, offset
        if getDepth(path) >= 3:
            pe = getParts(path)
        else:
            return -errno.ENOSYS 
        
        if getDepth(path) == 4 and ( pe[-2] == 'data' ):   
            table = Table(pe[-3], self.meta, schema=pe[-4], autoload=True)
            values = buf.split(',')
            a=pe[-1].split('#_')
            #u = table.update().\
            #                where(table.c[str(a[0])]==bindparam('old'+str(a[0]))).\
            #                values(table.c[str(a[0])]=bindparam('new'+str(a[0])))
            dk=[]
            dv={}
            for k in table.columns.keys():
                dk.append(k)
            i=0
            for v in values:
                dv[str(dk[i])]=v    
                i+=1
            dbg()
            ret = self.engine.execute(update(table,values=dv, whereclause=table.c[str(a[0])]==a[1]) )            
        elif getDepth(path) == 3 and ( pe[-1] == 'definition' ):
            pass#table = Table(pe[-2], self.meta, schema=pe[-3], autoload=True)
            #a = str(CreateTable(table).compile(self.engine))
        return len(buf)



def main():
    usage="""
          SQLFS: A filesystem to map any database server; to a filesystem:

          """ + fuse.Fuse.fusage

    server = SQLFS(version="%prog " + fuse.__version__,
                      usage=usage, dash_s_do='setsingle')
    server.parse(errex=1)
    server.flags = 0
    server.multithreaded = 1
    server.main()


if __name__ == '__main__':
    main()

