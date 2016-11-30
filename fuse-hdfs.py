#!/usr/bin/env python
import os, sys, errno
import pwd, grp, resource

from fuse import FUSE, FuseOSError, Operations
import pydoop.hdfs as hdfs

def log(f):
    '''Decorator function for debugging.'''
    def my_f(*args, **kwargs):
        print (f.__name__)
        print (args)
        return f(*args, **kwargs)
    return my_f

class PydoopDFS(Operations):
    def __init__(self):
        self.hdfs=hdfs.hdfs()
        self.filehandles={}
        self.fhmax=0

    def _h_g2G(self,group):
        try:
            return grp.getgrnam(group).gr_gid
        except KeyError:
            return grp.getgrnam('nobody').gr_gid
    def _G2h_g(self,gid):
        try:
            return grp.getgrgid(gid).gr_name
        except KeyError:
            return 'nobody'
    def _h_u2U(self,user):
        try:
            return pwd.getpwnam(user).pw_uid
        except KeyError:
           return pwd.getpwnam('nobody').pw_uid
    def _U2h_u(self,uid):
        try:
            return pwd.getpwuid(uid).pw_name
        except KeyError:
            return 'nobody'

    def access(self, path, mode):
        if not hdfs.access(path, mode):
           raise FuseOSError(errno.EACCES)

    @log
    def chmod(self, path, mode):
        hdfs.chmod(path, mode)

    def chown(self, path, uid, gid):
        '''Filter for unchanged uids and gids (-1)'''
        st = hdfs.stat(path)
        if uid == -1:
            if gid == -1:
                hdfs.chown(path)
            else:
                group= self._G2h_g(gid)
                hdfs.chown(path,group=group)
        else:
            user = self._U2h_u(uid)
            if gid == -1:
                hdfs.chown(path,user)
            else:
                group= self._G2h_g(gid)
                hdfs.chown(path, user, group)

    def getattr(self, path, fh=None):
        if not hdfs.path.exists(path):
            raise FuseOSError(errno.ENOENT)
        st = hdfs.stat(path)
        data={}
        data['st_atime']=st.st_atime
        data['st_ctime']=st.st_ctime
        data['st_gid']=self._h_g2G(st.st_gid)
        mode = st.st_mode
        if st.kind.lower() == 'directory':
            mode = mode + 16384
        if st.kind.lower() == 'file':
            mode = mode + 32768
        data['st_mode']=mode
        data['st_mtime']=st.st_mtime
        data['st_nlink']=st.st_nlink
        data['st_size']=st.st_size
        data['st_uid']=self._h_u2U(st.st_uid)
        return data

    def readdir(self, path, fh):
        '''Return a list of all files. This might be better to yield...'''
        return ['.','..']+[x.split(os.sep)[-1] for x in hdfs.ls(path)]

    @log
    def readlink(self, path):
        return hdfs.path.realpath(path)

    @log
    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EACCES)

    def rmdir(self, path):
        '''HDFS doesn't understand classic rmdir.'''
        if len(hdfs.ls(path)) > 0:
            raise FuseOSError(errno.ENOTEMPTY)
        else:
            hdfs.rmr(path)

    def mkdir(self, path, mode):
        hdfs.mkdir(path)

    def rename(self, old, new):
        hdfs.rename(old,new)

    def statfs(self, path):
        data={}
        data['f_bsize']=self.hdfs.default_block_size()
        data['f_blocks']=self.hdfs.capacity()/data['f_bsize']
        f_bused=self.hdfs.used()/data['f_bsize']
        data['f_bavail']=data['f_blocks']-f_bused
        data['f_bfree']=data['f_bavail']
        data['f_favail']=data['f_bavail']
        data['f_ffree']=data['f_bavail']
        data['f_files']=data['f_blocks']
        data['f_flag']=os.O_DSYNC
        data['f_frsize']=data['f_bsize']
        data['f_namemax']=255
        return data

    def unlink(self, path):
        hdfs.rmr(path)

    def symlink(self, name, target):
        '''Symlinks and hardlinks don't work.'''
        raise FuseOSError(errno.EACCES)

    def link(self, target, name):
        '''Symlinks and hardlinks don't work.'''
        raise FuseOSError(errno.EACCES)

    def utimens(self, path, times=None):
        return hdfs.utime(path, times)

    @log
    def open(self, path, flags):
        '''Open a file. This seems to reset perms for some odd reason.'''
        flags = flags & 1
        maxfh = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        if len(self.filehandles) == maxfh:
            raise FuseOSError(errno.EMFILE)
        while self.fhmax in self.filehandles.keys():
            self.fhmax = (self.fhmax+1)%maxfh
        self.filehandles[self.fhmax] = self.hdfs.open_file(path, flags)
        return self.fhmax

    @log
    def create(self, path, mode, fi=None):
        '''Open a nonexistent file. This will just create a new file and generate a
        new filehandle for you.'''
        mode = mode & 1
        maxfh = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        if len(self.filehandles) == maxfh:
            raise FuseOSError(errno.EMFILE)
        while self.fhmax in self.filehandles.keys():
            self.fhmax  = (self.fhmax+1)%maxfh
        hdfs.dump('',path)
        self.filehandles[self.fhmax] = self.hdfs.open_file(path, mode)
        return self.fhmax

    @log
    def read(self, path, length, offset, fh):
        f = self.filehandles[fh]
        f.seek(offset)
        return f.read(length)

    @log
    def write(self, path, buf, offset, fh):
        f = self.filehandles[fh]
        return f.write(f.read(offset)+buf)

    @log
    def truncate(self, path, length, fh=None):
        '''HDFS doesn't support truncate, so spoof it by writing at length.'''
        if fh is None:
            fh = self.fhmax
        f = self.filehandles[fh]
        f.write(f.read(length))

    @log
    def flush(self, path, fh):
        '''Flushing seems not to work for some reason.'''
        f = self.filehandles[fh]
#        f.flush()

    @log
    def release(self, path, fh):
        f = self.filehandles[fh]
        f.close()
        del self.filehandles[fh]

    @log
    def fsync(self, path, fdatasync, fh):
        '''Flushing seems not to work for some reason.'''
        f = self.filehandles[fh]
#        return f.flush()


def main(mountpoint):
    FUSE(PydoopDFS(), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
