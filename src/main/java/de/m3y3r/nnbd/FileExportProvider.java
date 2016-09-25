package de.m3y3r.nnbd;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileExportProvider implements ExportProvider {

	private File basePath;

	private RandomAccessFile raf;
	private FileChannel channel;

	public FileExportProvider(File basePath) {
		this.basePath = basePath;
	}

	@Override
	public long open(CharSequence exportName) throws IOException {
		System.out.println("basePath=" + basePath.getAbsolutePath());

		File file = new File(basePath, exportName + ".img");
		System.out.println("file=" + file.getAbsolutePath());
		raf = new RandomAccessFile(file, "rw");
		channel = raf.getChannel();
		return channel.size();
	}

	@Override
	public void close() throws IOException {
		channel.close();
		raf.close();
	}

	@Override
	public ByteBuffer read(CharSequence exportName, long offset, long length) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate((int) length);
		channel.position(offset);
		channel.read(bb);
		bb.flip();
		return bb;
	}

	@Override
	public void write(CharSequence exportName, long offset, long length, ByteBuffer message) throws IOException {
		assert message != null;
		assert length == message.remaining();

		channel.position(offset);
		channel.write(message);
	}

	@Override
	public void flush(CharSequence exportName) throws IOException {
		channel.force(true);
	}

	@Override
	public void trim(CharSequence exportName) {
		throw new UnsupportedOperationException();
	}
}
