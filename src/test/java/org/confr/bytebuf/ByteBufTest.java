package org.confr.bytebuf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.Test;

public class ByteBufTest {
    @Test
    public void testreferenceCounted() {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeInt(1234);
        buffer.writeInt(2345);

        ByteBuf readSlice = buffer.readSlice(2);

        ByteBuf readRetainedSlice = buffer.readRetainedSlice(2);
        ByteBuf anotherBuf = buffer;
        System.out.println("end");
    }
}
