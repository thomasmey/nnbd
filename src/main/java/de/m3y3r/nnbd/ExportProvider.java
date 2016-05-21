package de.m3y3r.nnbd;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ExportProvider extends Closeable {

	long open(String exportName) throws IOException;
	ByteBuffer read(String exportName, long offset, long length) throws IOException;
	void write(String exportName, long offset, long length, ByteBuffer message)throws IOException;
	void flush(String exportName)throws IOException;
	void trim(String exportName)throws IOException;
}
