package de.m3y3r.nnbd.ep.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import de.m3y3r.nnbd.ep.ExportProvider;

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
	public ByteBuffer read(long offset, long length, boolean sync) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate((int) length);
		channel.position(offset);
		channel.read(bb);
		bb.flip();
		return bb;
	}

	@Override
	public void write(long offset, ByteBuffer message, boolean sync) throws IOException {
		assert message != null;

		channel.position(offset);
		channel.write(message);
	}

	@Override
	public void flush() throws IOException {
		channel.force(true);
	}

	@Override
	public void trim() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String create(CharSequence exportName, long size) throws IOException {
		return null;
	}
}
