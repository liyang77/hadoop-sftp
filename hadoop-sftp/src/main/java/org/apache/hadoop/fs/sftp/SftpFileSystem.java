/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package org.apache.hadoop.fs.sftp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class SftpFileSystem extends FileSystem {
	public static final Log LOG = LogFactory.getLog(SftpFileSystem.class);

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

	public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
	public static final String FS_SFTP_USER_PREFIX = "fs.sftp.user.";
    public static final String FS_SFTP_PASS_PREFIX = "fs.sftp.pass.";
    public static final String FS_SFTP_KEY_PREFIX = "fs.sftp.key.";
	public static final String FS_SFTP_HOST = "fs.sftp.host";
	public static final String FS_SFTP_HOST_PORT = "fs.sftp.host.port";
	public static final int SFTP_DEFAULT_PORT = 22;
	public static final String E_SAME_DIRECTORY_ONLY = "only same directory renames are supported";

	private URI uri;
	
	/**
	 * Return the protocol scheme for the FileSystem.
	 * <p/>
	 *
	 * @return <code>sftp</code>
	 */
	@Override
	public String getScheme() {
		return "sftp";
	}

	/**
	 * Get the default port for this SftpFileSystem.
	 *
	 * @return the default port
	 */
	@Override
	protected int getDefaultPort() {
		return SFTP_DEFAULT_PORT;
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException { // get
		super.initialize(uri, conf);
	    String host = uri.getHost();
	    host = (host == null) ? conf.get(FS_SFTP_HOST, null) : host;
	    if (host == null) {
	      throw new IOException("Invalid host specified");
	    }
	    conf.set(FS_SFTP_HOST, host);

	    int port = uri.getPort();
	    port = (port == -1) ? SFTP_DEFAULT_PORT : port;
	    conf.setInt("fs.sftp.host.port", port);

	    String user = uri.getUserInfo();
	    if (user == null) {
	    	user = conf.get(FS_SFTP_USER_PREFIX + host, null);
	    }
	    conf.set(FS_SFTP_USER_PREFIX + host, user);
	    setConf(conf);
	    this.uri = uri;
	}

	/**
	 * Connect to the sftp server using configuration parameters *
	 * 
	 * @return A ChannelSftp instance
	 * @throws IOException
	 */
	private ChannelSftp connect() throws IOException {
		JSch jsch=new JSch();
		Configuration conf = getConf();
		String host = conf.get(FS_SFTP_HOST);
		int port = conf.getInt(FS_SFTP_HOST_PORT, SFTP_DEFAULT_PORT);
		String user = conf.get(FS_SFTP_USER_PREFIX + host);
		try {
			Session session = jsch.getSession(user, host, port);
		    String pass = conf.get(FS_SFTP_PASS_PREFIX + host);

		    if (pass != null) {
		        session.setPassword(pass);
		    } else {
	            String identityPath = conf.get(FS_SFTP_KEY_PREFIX + host, "~/.ssh/id_rsa");
	            addIdentity(jsch, user, identityPath);
		    }
            Properties props = new Properties(); 
            props.put("StrictHostKeyChecking", "no");
            session.setConfig(props);
			session.connect();
			Channel channel=session.openChannel("sftp");
			channel.connect();
		    return (ChannelSftp) channel;
		} catch (JSchException e) {
			throw new IOException("Login failed on server " + e.getLocalizedMessage());
		}

	}
	
	/**
	 * Providing private key for authentication
	 * @param jsch
	 * @param user
	 * @param identityPath
	 */
    private void addIdentity(JSch jsch, String user, String identityPath) {
        Path prvPath = new Path(identityPath);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            char[] charArray = new char[10240];
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(prvPath)));
            int len = br.read(charArray, 0, 10240);
            br.close();
            byte[] prvKey = (new String(charArray,0, len)).getBytes();
            jsch.addIdentity(user, prvKey, null, new byte[0]);
        } catch (IOException | JSchException e) {
            LOG.warn(e.getClass().getName());
        }
    }
    
	/**
	 * Logout and disconnect the given ChannelSftp. *
	 * 
	 * @param channel
	 * @throws IOException
	 */
	private void disconnect(ChannelSftp channel) throws IOException {
	    if (channel != null) {
	      if (channel.isConnected()) {
	          channel.disconnect();
	      }
          try {
            Session session = channel.getSession();
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        } catch (JSchException e) {
        }

	    }
	  }
	
	/**
	 * Resolve against given working directory. *
	 * 
	 * @param workDir
	 * @param path
	 * @return
	 */
	private Path makeAbsolute(Path workDir, Path path) {
	    if (path.isAbsolute()) {
	      return path;
	    }
	    return new Path(workDir, path);
	}
	
	/**
	 * Return the absolute path of the file.
	 * @param file
	 * @return
	 */
	private String getPathName(Path file) {
	    return file.toUri().getPath();
	}
	
	@Override
	public FSDataInputStream open(Path file, int bufferSize) throws IOException {
		ChannelSftp channel = connect();
		try {
			InputStream is = channel.get(getPathName(file));
			FSDataInputStream fis = new FSDataInputStream(new SftpInputStream(is, channel, statistics));
			return fis;
		} catch (SftpException e) {
			throw new IOException("Unable to open " + e.getLocalizedMessage());
		}
	}

	@Override
	public FSDataOutputStream create(Path file, FsPermission permission,
		      boolean overwrite, int bufferSize, short replication, long blockSize,
		      Progressable progress)
			throws IOException {
		try {
			final ChannelSftp channel = connect();
			Path workDir = new Path(channel.pwd());
			Path absolute = makeAbsolute(workDir, file);
			FileStatus status;
			try {
				status = getFileStatus(channel, file);
			} catch (IOException fnfe) {
				status = null;
			}
			if (status != null) {
				if (overwrite && !status.isDirectory()) {
					delete(channel, file, false);
				} else {
					disconnect(channel);
					throw new FileAlreadyExistsException("File already exists: " + file);
				}
			}
	    
			Path parent = absolute.getParent();
			if (parent == null || !mkdirs(channel, parent, FsPermission.getDirDefault())) {
				parent = (parent == null) ? new Path("/") : parent;
				disconnect(channel);
				throw new IOException("create(): Mkdirs failed to create: " + parent);
			}
			OutputStream os = channel.put(getPathName(file));

			FSDataOutputStream fos = new FSDataOutputStream(os, statistics);
			return fos;
		} catch (Exception e) {
			throw new IOException("create(): failed to create " + e.getLocalizedMessage());
		}
	}
	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
			throws IOException {
	    throw new IOException("Not supported");
	}
	
	private boolean exists(ChannelSftp channel, Path file) throws IOException {
	    try {
	      getFileStatus(channel, file);
	      return true;
	    } catch (FileNotFoundException fnfe) {
	      return false;
	    }
	}
	  
	@Override
	public boolean delete(Path file, boolean recursive) throws IOException {
		ChannelSftp channel = connect();
		try {
		    boolean success = delete(channel, file, recursive);
		    return success;
		} finally {
		    disconnect(channel);
		}
	}

	private boolean delete(ChannelSftp channel, Path file, boolean recursive)
	      throws IOException {
		try {
			Path workDir = new Path(channel.pwd());
			Path absolute = makeAbsolute(workDir, file);
			String pathName = absolute.toUri().getPath();
			try {
				FileStatus fileStat = getFileStatus(channel, absolute);
				if (fileStat.isFile()) {
					channel.rm(pathName);
					return true;
				}
			} catch (FileNotFoundException e) {
				//the file is not there
				return false;
			}
			FileStatus[] dirEntries = listStatus(channel, absolute);
			if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
				throw new IOException("Directory: " + file + " is not empty.");
			}
			for (FileStatus dirEntry : dirEntries) {
				delete(channel, new Path(absolute, dirEntry.getPath()), recursive);
			}
			channel.rmdir(pathName);
			return true;
		} catch (SftpException e) {
			return false;
		}
	}
	  
	@Override
	public URI getUri() {
	    return uri;
	}
	
	@Override
	public FileStatus[] listStatus(Path file) throws FileNotFoundException,
			IOException {
		ChannelSftp channel = connect();
		try {
			FileStatus[] stats = listStatus(channel, file);
		      return stats;
		} finally {
		    disconnect(channel);
		}
	}
	
    @SuppressWarnings("unchecked")
    private FileStatus[] listStatus(ChannelSftp channel, Path file)
          throws IOException {
        try {
            
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, file);
            FileStatus fileStat = getFileStatus(channel, absolute);
            if (fileStat.isFile()) {
                return new FileStatus[] { fileStat };
            }
            Vector<ChannelSftp.LsEntry> lsEntries = channel.ls(absolute.toUri().getPath());
            List<FileStatus> fileStats = new ArrayList<FileStatus>();
            for (ChannelSftp.LsEntry lsEntry: lsEntries) {
                String fileName = lsEntry.getFilename();
                if (!".".equals(fileName) && !"..".equals(fileName)) {
                     try {
                         fileStats.add(getFileStatus(lsEntry.getAttrs(), new Path(file, fileName)));
                     } catch (Exception e) {
                         LOG.info("failed to open " + file.toString() + " " + fileName);
                     }
                }
            }
            return fileStats.toArray(new FileStatus[fileStats.size()]);
        } catch (SftpException e) {
            throw new IOException("unable to list status: " + e.getLocalizedMessage() );
        }
    }
      
    @Override
    public FileStatus getFileStatus(Path file) throws IOException {
        ChannelSftp channel = connect();
        try {
            FileStatus status = getFileStatus(channel, file);
            return status;
        } finally {
              disconnect(channel);
        }
    }
    
    private FileStatus getFileStatus(ChannelSftp channel, Path file)
          throws IOException {
        try {
            SftpATTRS sftpAttrs = channel.lstat(getPathName(file));
            return getFileStatus(sftpAttrs, file);
        } catch (SftpException e) {
            throw new IOException("getFileStatus failed: " + e.getLocalizedMessage());
        }
    }

    /**
     * Convert the file information in SftpATTRS to a {@link FileStatus} object. *
     * 
     * @param sftpAttrs
     * @param filePath
     * @return FileStatus
     */
    private FileStatus getFileStatus(SftpATTRS sftpAttrs, Path filePath) {
        long length = sftpAttrs.getSize();
        boolean isDir = sftpAttrs.isDir();
        int blockReplication = 1;
        // Using default block size
        long blockSize = DEFAULT_BLOCK_SIZE;
        long modTime = sftpAttrs.getMTime();
        long accessTime = sftpAttrs.getATime();
        FsPermission permission = new FsPermission((short)sftpAttrs.getPermissions());
        String user = Integer.toString(sftpAttrs.getUId());
        String group = Integer.toString(sftpAttrs.getGId());
        return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
            accessTime, permission, user, group, filePath);
    }

    @Override
    public boolean mkdirs(Path file, FsPermission permission) throws IOException {
        ChannelSftp channel = connect();
        try {
          boolean success = mkdirs(channel, file, permission);
          return success;
        } finally {
          disconnect(channel);
        }
    }

    private boolean mkdirs(ChannelSftp channel, Path file, FsPermission permission)
          throws IOException {
        try {
            Path workDir = new Path(channel.pwd());
            Path absolute = makeAbsolute(workDir, file);
            String pathName = absolute.getName();
            if (!exists(channel, absolute)) {
                Path parent = absolute.getParent();
                if (parent != null) {
                    mkdirs(channel, parent, FsPermission.getDirDefault());
            }
                String parentDir = parent.toUri().getPath();
                channel.cd(parentDir);
                channel.mkdir(pathName);
            } else if (isFile(channel, absolute)) {
                throw new ParentNotDirectoryException(String.format(
                        "Can't make directory for path %s since it is a file.", absolute));
            }
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    private boolean isFile(ChannelSftp channel, Path file) {
        try {
          return getFileStatus(channel, file).isFile();
        } catch (IOException e) {
          return false; // file does not exist
        }
    }
      
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        ChannelSftp channel = connect();
        try {
            Path workDir = new Path(channel.pwd());
            Path absoluteSrc = makeAbsolute(workDir, src);
            Path absoluteDst = makeAbsolute(workDir, dst);
            channel.rename(absoluteSrc.getName(), absoluteDst.getName());
            return true;
        } catch (SftpException e) {
            return false;
        } finally {
          disconnect(channel);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return getHomeDirectory();
    }
    
    @Override
    public Path getHomeDirectory() {
        ChannelSftp channel = null;
        Path homeDir = null;
        try {
            channel = connect();
            homeDir = new Path(channel.pwd());
        } catch (SftpException|IOException e) {
        } finally {
            try {
                disconnect(channel);
            } catch (Exception e) {
            }
        }
        return homeDir==null?super.getHomeDirectory():homeDir;
        
    }

    @Override
    public void setWorkingDirectory(Path path) {
    }

}