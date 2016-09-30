package de.m3y3r.nnbd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

public class NbdTransmissionInboundHandler extends ByteToMessageDecoder {

	private enum State {TM_RECEIVE_CMD, TM_RECEIVE_CMD_DATA};
	private State state = State.TM_RECEIVE_CMD;

	private static class Error {
		public final static int EIO = 5;
	}

	private short cmdFlags;
	private short cmdType;
	private long cmdHandle;
	private long cmdOffset;
	private long cmdLength;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		for(;;) {
			switch(state) {
			case TM_RECEIVE_CMD:
				if(!hasMin(in, 28))
					return;

				receiveTransmissionCommand(ctx, in);
				state = State.TM_RECEIVE_CMD_DATA;
				break;

			//FIXME: this will buffer maybe a lot of bytes?!
			case TM_RECEIVE_CMD_DATA:
				if(cmdType == Protocol.NBD_CMD_WRITE && !hasMin(in, (int) cmdLength))
					return;

				processOption(ctx, in);
				state = State.TM_RECEIVE_CMD;
				break;
			}
		}
	}

	private static boolean hasMin(ByteBuf in, int wanted) {
			return in.readableBytes() >= wanted;
	}

	private void processOption(ChannelHandlerContext ctx, ByteBuf in) throws IOException {

		switch(cmdType) {
		case Protocol.NBD_CMD_READ:
		{
			final long cmdOffset = this.cmdOffset;
			final long cmdLength = this.cmdLength;
			final long cmdHandle = this.cmdHandle;
			Runnable operation = () -> {
				ExportProvider ep = ChannelManager.INSTANCE.getExportProvider(ctx.channel());
				ByteBuffer data = null;
				int err = 0;
				try {
					//FIXME: use FUA/sync flag correctly
					data = ep.read(cmdOffset, cmdLength, false);
				} catch(IOException e) {
					err = Error.EIO;
				} finally {
					sendTransmissionSimpleReply(ctx, err, cmdHandle, Unpooled.wrappedBuffer(data));
				}
			};
			GlobalEventExecutor.INSTANCE.execute(operation);
			break;
		}
		case Protocol.NBD_CMD_WRITE:
		{
			final long cmdOffset = this.cmdOffset;
			final long cmdLength = this.cmdLength;
			final long cmdHandle = this.cmdHandle;
			final ByteBuf buf = in.readBytes((int) cmdLength);
			Runnable operation = () -> {
				ExportProvider ep = ChannelManager.INSTANCE.getExportProvider(ctx.channel());
				int err = 0;
				try {
					//FIXME: use FUA/sync flag correctly
					ep.write(cmdOffset, buf.nioBuffer(), false);
				} catch(IOException e) {
					err = Error.EIO;
				} finally {
					sendTransmissionSimpleReply(ctx, err, cmdHandle, null);
				}
			};
			GlobalEventExecutor.INSTANCE.execute(operation);
			break;
		}
		case Protocol.NBD_CMD_DISC:
		{
 			ctx.channel().close();
			break;
		}
		case Protocol.NBD_CMD_FLUSH:
		{
			/* we must drain all NBD_CMD_WRITE and NBD_WRITE_TRIM from the queue
			 * before processing NBD_CMD_FLUSH
			 */
//			exportProvider.flush(exportName);
			sendTransmissionSimpleReply(ctx, 0, cmdHandle, null);
			break;
		}
		case Protocol.NBD_CMD_TRIM:
		{
//			exportProvider.trim(exportName);
			sendTransmissionSimpleReply(ctx, 0, cmdHandle, null);
			break;
		}
		default:
			sendTransmissionSimpleReply(ctx, Protocol.NBD_REP_ERR_INVALID, cmdHandle, null);
		}
	}

	private void receiveTransmissionCommand(ChannelHandlerContext ctx, ByteBuf message) throws IOException {
		if(message.readInt() != 0x25609513) {
			throw new IllegalArgumentException("Invalid request magic!");
		}

		cmdFlags = message.readShort();
		cmdType = message.readShort();
		cmdHandle = message.readLong();
		cmdOffset = message.readLong(); //FIXME: unsigned!
		cmdLength = message.readUnsignedInt();
	}

	private static void sendTransmissionSimpleReply(ChannelHandlerContext ctx, int error, long handle, ByteBuf data) {
		ByteBuf bbr = ctx.alloc().buffer(16);
		bbr.writeInt(0x67446698);
		bbr.writeInt(error); // zero for okay
		bbr.writeLong(handle);

		ctx.write(bbr);
		if(data != null) {
			ctx.write(data);
		}
		ctx.flush();
	}
}
