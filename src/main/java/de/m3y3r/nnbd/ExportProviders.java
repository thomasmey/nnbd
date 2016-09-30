package de.m3y3r.nnbd;

import java.io.File;

public class ExportProviders {
	public static ExportProvider getNewDefault() {
		return new FileExportProvider(new File("nbd-server"));
	}
}
