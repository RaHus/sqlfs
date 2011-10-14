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


from util import dbg

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select#, update
from sqlalchemy.sql.expression import update

#from beaker.cache import CacheManager
#from beaker.util import parse_cache_config_options

from sqlfsbackend import CsvBackend

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



class SQLFS(Fuse):
    """
    Sql to filesystem mapper. 
    """

    backend = None

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.backend = CsvBackend('mysql://root:1ikko0ll@localhost/')


    def getattr(self, path):
        """
        """
        return self.backend.getattr(path)        


    def readdir(self, path, offset):
        """
        return: [[('file1', 0), ('file2', 0), ... ]]
        """
        return self.backend.readdir(path,offset)


    def read ( self, path, length, offset ):
        print '*** read', path, length, offset
        return self.backend.read(path, length, offset)


    def write ( self, path, buf, offset ):
        print '*** write', path, buf, offset
        return self.backend.write(path,buf,offset)

        
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

