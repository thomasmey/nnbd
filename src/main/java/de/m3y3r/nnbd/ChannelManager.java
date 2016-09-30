package de.m3y3r.nnbd;

import io.netty.channel.Channel;

public enum ChannelManager {

	INSTANCE;

	private ExportProvider ep;

	public ExportProvider getExportProvider(Channel channel) {
		return ep;
	}

	public void addExportProvider(Channel channel, ExportProvider ep,
			int clientFlags) {
		this.ep = ep;
	}

}
