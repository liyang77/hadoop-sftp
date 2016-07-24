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
 */
package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SftpInputStream extends FSInputStream {

    InputStream wrappedStream;
    ChannelSftp channel;
    FileSystem.Statistics stats;
    boolean closed;
    long pos;

    public SftpInputStream(InputStream stream, ChannelSftp channel,
            FileSystem.Statistics stats) {
          if (stream == null) {
            throw new IllegalArgumentException("Null InputStream");
          }
          if (channel == null || !channel.isConnected()) {
            throw new IllegalArgumentException("FTP channel null or not connected");
          }
          this.wrappedStream = stream;
          this.channel = channel;
          this.stats = stats;
          this.pos = 0;
          this.closed = false;
    }
    
    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new IOException("Seek not supported");
    }


    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException("Seek not supported");
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
          }

          int byteRead = wrappedStream.read();
          if (byteRead >= 0) {
            pos++;
          }
          if (stats != null && byteRead >= 0) {
            stats.incrementBytesRead(1);
          }
          return byteRead;
    }

    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }

      int result = wrappedStream.read(buf, off, len);
      if (result > 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }

      return result;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }
      super.close();
      closed = true;
      if (!channel.isConnected()) {
        throw new IOException("Client not connected");
      }
      Session session = null;
      try {
          session = channel.getSession();
      } catch (JSchException e) {
      }
      channel.disconnect();
      if (session != null) {
          session.disconnect();
      }
    }

    // Not supported.

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void mark(int readLimit) {
      // Do nothing
    }

    @Override
    public void reset() throws IOException {
      throw new IOException("Mark not supported");
    }
}
