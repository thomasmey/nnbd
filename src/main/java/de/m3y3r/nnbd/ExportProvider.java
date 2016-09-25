package de.m3y3r.nnbd;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ExportProvider extends Closeable {

	long open(CharSequence exportName) throws IOException;
	ByteBuffer read(CharSequence exportName, long offset, long length) throws IOException;
	void write(CharSequence exportName, long offset, ByteBuffer message)throws IOException;
	void flush(CharSequence exportName)throws IOException;
	void trim(CharSequence exportName)throws IOException;
}
