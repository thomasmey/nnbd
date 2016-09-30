package de.m3y3r.nnbd;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ExportProvider extends Closeable {

	long open(CharSequence exportName) throws IOException;
	ByteBuffer read(long offset, long length, boolean sync) throws IOException;
	void write(long offset, ByteBuffer message, boolean sync) throws IOException;
	void flush() throws IOException;
	void trim() throws IOException;
}
