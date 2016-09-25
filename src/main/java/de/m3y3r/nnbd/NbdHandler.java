package de.m3y3r.nnbd;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class NbdHandler extends ChannelInboundHandlerAdapter {

	private enum State {HS_CLIENT_FLAGS, HS_OPTION_HAGGLING, TM_RECEIVE_CMD, TM_CMD_WRITE};
	private State state;

	private short handshakeFlags;
	private String exportName;

	private ExportProvider exportProvider;

	private int clientFlags;

	/* data from currently executing commmand */
	private long cmdLength;
	private long cmdHandle;
	private long cmdOffset;

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		exportProvider = new FileExportProvider(new File("nbd-server"));
		initHandshake(ctx);
		state = State.HS_CLIENT_FLAGS;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf bb = (ByteBuf) msg;

		while(bb.readableBytes() > 0) {
			/* The NBD protocol has two phases: the handshake (HS_) and the transmission (TM_) */
			switch(state) {
			case HS_CLIENT_FLAGS:
				receiveHandshakeClientFlag(ctx, bb);
				state = State.HS_OPTION_HAGGLING;
				break;
			case HS_OPTION_HAGGLING:
				/* state transition happens in method! */
				receiveHandshakeOption(ctx, bb);
				break;
			case TM_RECEIVE_CMD:
				receiveTransmissionCommand(ctx, bb);
				break;

			/* awkward implementation, isn't there another way to read more bytes while in this state?! */
			case TM_CMD_WRITE:
				long avail = bb.readableBytes();
				if(avail <= cmdLength) {
					exportProvider.write(exportName, cmdOffset, bb.readBytes((int) avail).nioBuffer());
					cmdLength -= avail;
					cmdOffset += avail;
				} else {
					exportProvider.write(exportName, cmdOffset, bb.readBytes((int) cmdLength).nioBuffer());
					cmdLength = 0;
				}

				/* are we finished? send reply and switch back to cmd receive state */
				if(cmdLength == 0) {
					sendTransmissionSimpleReply(ctx, 0, cmdHandle, null);
					state = State.TM_RECEIVE_CMD;
				}
				break;
			}
		}

		ReferenceCountUtil.release(msg);
	}

	public void detach() throws IOException {
		exportProvider.close();
	}

	private void initHandshake(ChannelHandlerContext ctx) throws IOException {
		/* initiate handshake */
		ByteBuffer bb = ByteBuffer.allocate(20);
		bb.putLong(0x4e42444d41474943l);
		bb.putLong(0x49484156454F5054l);

		// "handshake flags"
		handshakeFlags = Protocol.NBD_FLAG_FIXED_NEWSTYLE & Protocol.NBD_FLAG_NO_ZEROES;
		bb.putShort(handshakeFlags);
		bb.flip();

		ctx.writeAndFlush(Unpooled.wrappedBuffer(bb));
	}

	private void receiveTransmissionCommand(ChannelHandlerContext ctx, ByteBuf message) throws IOException {
		if(message.readInt() != 0x25609513) {
			throw new IllegalArgumentException();
		}

		short cmdFlags = message.readShort();
		short cmdType = message.readShort();
		long cmdHandle = message.readLong();
		long cmdOffset = message.readLong(); //FIXME: unsigned!
		long cmdLength = message.readUnsignedInt();

		switch(cmdType) {
		case Protocol.NBD_CMD_READ:
		{
			ByteBuffer data = exportProvider.read(exportName, cmdOffset, cmdLength);
			sendTransmissionSimpleReply(ctx, 0, cmdHandle, data);
			break;
		}
		case Protocol.NBD_CMD_WRITE:
		{
			this.cmdLength = cmdLength;
			this.cmdHandle = cmdHandle;
			this.cmdOffset = cmdOffset;
			this.state = State.TM_CMD_WRITE;
			break;
		}
		case Protocol.NBD_CMD_DISC:
		{
 			ctx.close();
			break;
		}
		case Protocol.NBD_CMD_FLUSH:
		{
			exportProvider.flush(exportName);
			sendTransmissionSimpleReply(ctx, 0, cmdHandle, null);
			break;
		}
		case Protocol.NBD_CMD_TRIM:
		{
			exportProvider.trim(exportName);
			sendTransmissionSimpleReply(ctx, 0, cmdHandle, null);
			break;
		}
		default:
			sendTransmissionSimpleReply(ctx, Protocol.NBD_REP_ERR_INVALID, cmdHandle, null);
		}
	}

	private void sendTransmissionSimpleReply(ChannelHandlerContext ctx, int error, long handle, ByteBuffer data) throws IOException {
		ByteBuffer bbr = ByteBuffer.allocate(16);
		bbr.putInt(0x67446698);
		bbr.putInt(error);
		bbr.putLong(handle);
		bbr.flip();

		ctx.write(Unpooled.wrappedBuffer(bbr));
		if(data != null) {
			ctx.write(Unpooled.wrappedBuffer(data));
		}
		ctx.flush();
	}

	private void receiveHandshakeClientFlag(ChannelHandlerContext ctx, ByteBuf message) throws IOException {
		clientFlags = message.readInt();
		if((clientFlags & Protocol.NBD_FLAG_FIXED_NEWSTYLE) == 0)
			ctx.close();
	}

	private CharSequence receiveHandshakeOption(ChannelHandlerContext ctx, ByteBuf message) throws IOException {

		if(message.readLong() != 0x49484156454F5054l)
			throw new IllegalArgumentException();

		int option = message.readInt();
		long optionLen = message.readUnsignedInt();

		switch(option) {
		case Protocol.NBD_OPT_EXPORT_NAME:
			assert optionLen >= message.readableBytes();
			CharSequence exportName = message.readCharSequence((int) optionLen, Charset.forName("UTF-8"));

			long exportSize;
			if((exportSize = exportProvider.open(exportName)) < 0) {
				ctx.close();
				break;
			}

			/* build response */
			ByteBuffer resp = ByteBuffer.allocate(256);
			resp.putLong(exportSize);
			short transmissionFlags = Protocol.NBD_FLAG_HAS_FLAGS | Protocol.NBD_FLAG_SEND_FLUSH;
			resp.putShort(transmissionFlags);

			if((clientFlags & Protocol.NBD_FLAG_NO_ZEROES) == 0) {
				resp.position(resp.position() + 124);
			}
			resp.flip();
			ctx.writeAndFlush(Unpooled.wrappedBuffer(resp));

			state = State.TM_RECEIVE_CMD;
			return exportName;

		default:
			sendHandshakeOptionHagglingReply(ctx, option, Protocol.NBD_REP_ERR_UNSUP, null);
		}

		return null;
	}

	private void sendHandshakeOptionHagglingReply(ChannelHandlerContext ctx, int option, int error, ByteBuffer data) throws IOException {

		ByteBuffer optionReply = ByteBuffer.allocate(18);
		optionReply.putLong(0x3e889045565a9l);
		optionReply.putInt(option);
		optionReply.putInt(error);
		int len = 0;
		if(data != null)
			len = data.remaining();
		optionReply.putInt(len);
		optionReply.flip();
		ctx.write(optionReply);
		if(data != null)
			ctx.write(data);
	}
}
