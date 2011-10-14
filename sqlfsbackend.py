from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import update, insert

from util import *

from time import time

from fuse import Fuse
import fuse 
fuse.fuse_python_api = (0, 2)

import stat    # for file properties
import os      # for filesystem modes (O_RDONLY, etc)
import errno   # for error number codes (ENOENT, etc)
               # - note: these must be returned as negatives

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



class Backend(object):

    engine = None
    insp = None
    meta = None

    def __init__(self,dburi):
        self.engine = create_engine(dburi)#'mysql://root:1ikko0ll@localhost/'
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.insp = Inspector.from_engine(self.engine)
        print 'Init complete.'

    def layout(self,path):
        """
        Return true if path is part of the layout
        """
        if path == "." or path== "..":
            return True
        else: 
            return False

    def getattr(self,path):
        pass

    def read(self,path,offset):
        pass

    def write(self,path,buf,offset):
        pass

    def readtable(self,path):
        pass

    def writetable(self,path):
        pass

    def get_definition(self, path):
        pe = getParts(path)
        table = Table(pe[-2], self.meta, schema=pe[-3], autoload=True)
        return str(CreateTable(table).compile(self.engine))

    def readdir(self,path,offset):
        pass

    def mknod ( self, path, mode, dev ):
        print '*** mknod', path, oct(mode), dev
        return -errno.ENOSYS

class CsvBackend(Backend):
    
    def __init__(self,dburi):
        Backend.__init__(self,dburi)

    def layout(self,path):
        pe = path.split('/')[1:]
        try:
            if path == '/':
                return True
            #Schema dir list
            elif getDepth(path) == 1 and pe[-1] in self.insp.get_schema_names():
                print pe[-1]+"\n"
                return True
            #Table dir list for a schema
            elif getDepth(path) == 2 and pe[-1] in self.insp.get_table_names(schema=pe[-2]):
                print pe[-1]+"\n"
                return True
            #Table definition
            elif getDepth(path) == 3 and ( pe[-1] == 'define' ):
                return True
            #Table data
            elif getDepth(path) == 3 and ( pe[-1] == 'data' ):
                return True
            elif getDepth(path) == 4 and ( pe[-2] == 'data' ): #and pe[-1]=regexp("^column#_"):
                return True

        except Exception, e:
            print e
        return False

    def getattr(self, path):
        """
        """
        if not self.layout(path):
            return -errno.ENOENT #Error Not Found

        pe = path.split('/')[1:]
        st = MyStat()
        st.st_atime = int(time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime

        try:
            if getDepth(path) == 3 and ( pe[-1] == 'define' ):
                st.st_mode = stat.S_IFREG | 0666
                st.st_nlink = 1
                st.st_size = len(self.get_definition(path))
            
            elif getDepth(path) == 4 and ( pe[-2] == 'data' ):
                st.st_mode = stat.S_IFREG | 0666
                st.st_nlink = 1
                st.st_size=len(self.get_filerow(path))
                if st.st_size==0:
                    return -errno.ENOENT
        except KeyError, k:
            print k
            return -errno.ENOENT
        return st


    def read(self,path, length, offset):
        print '*** read', path, length, offset
        if not self.layout(path):
            return -errno.ENOENT #Error Not Found

        if getDepth(path) >= 2:
            pe = getParts(path)
        else:
            return -errno.ENOENT 
        
        if getDepth(path) == 4 and ( pe[-2] == 'data' ):   
            a=self.get_filerow(path)
        elif getDepth(path) == 3 and ( pe[-1] == 'define' ):
            a=self.get_definition(path)
        return a[offset:offset+length]

    def write(self,path,buf,offset):
        print '*** write', path#, buf, offset

        #if not self.layout(path):
        #    return -errno.ENOSYS #Error Not Found

        if getDepth(path) >= 2:
            pe = getParts(path)
        else:
            return -errno.ENOSYS 
        
        if getDepth(path) == 4 and ( pe[-2] == 'data' ):   
            table = Table(pe[-3], self.meta, schema=pe[-4], autoload=True)
            values = buf.split(',')
            a=pe[-1].split('#_')
            dv=dict(zip(table.columns.keys(), values))
            ret = self.engine.execute(update(table,values=dv, whereclause=table.c[str(a[0])]==a[1]) )
            if ret:
                ret = self.engine.execute(insert(table,values=dv) )
        elif getDepth(path) == 3 and ( pe[-1] == 'define' ):
            pass#table = Table(pe[-2], self.meta, schema=pe[-3], autoload=True)
            #a = str(CreateTable(table).compile(self.engine))
        return len(buf)

    def mknod ( self, path, mode, dev ):
        print '*** mknod', path, oct(mode), dev
#        if getDepth(path) >= 2:
#            pe = getParts(path)
#        else:
#            return -errno.ENOSYS 
#        
#        if getDepth(path) == 4 and ( pe[-2] == 'data' ):   
#            table = Table(pe[-3], self.meta, schema=pe[-4], autoload=True)
#            if pe[-1].contains('#_'):
#            values = ["" for e in table.columns.keys()]
#            dv=dict(zip(table.columns.keys(), values))
#            #dbg()
#            ret = self.engine.execute(insert(table,values=dv, whereclause=table.c[str(a[0])]==a[1]) )            
                    

    def readdir(self,path,offset):
        if not self.layout(path):
            yield -errno.ENOENT 

        pe = path.split('/')[1:]
        dirents = [ '.', '..' ]
        print '#########readdir: ',path,offset,pe[-1]
        if path == '/':
            dirents.extend(self.insp.get_schema_names())
        elif getDepth(path) == 1:
            dirents.extend([str(entry) for entry in self.insp.get_table_names(schema=pe[-1])] )
        elif getDepth(path) == 2:
            dirents.extend([ 'define', 'data' ])
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

    def get_filerow(self, path):
        pe = getParts(path)
        try:
            table = Table(pe[-3], self.meta, schema=pe[-4], autoload=True)
            a=pe[-1].split('#_')
            ret = self.engine.execute(select(['*'],table.c[str(a[0])]==a[1])).fetchone()
            return ', '.join(str(e) for e in ret)+"\n"
        except Exception, e:
            print e
        return ''

#class AdminBackend(Backend):
#class XMLBackend(Backend):
#class HTMLBackend(Backend):
#class ScriptBackend(Backend):
#class JsonBackend(Backend):
