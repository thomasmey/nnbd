package de.m3y3r.nnbd;

public class ByteUtil {

	public static long u32ToLong(int v) {
		return (long)v & 0xff_ff_ff_ffl;
	}

	public static int u16ToInt(short v) {
		return v & 0xff_ff;
	}

}
