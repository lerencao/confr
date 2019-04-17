package org.confr.codec;

import org.confr.messages.Ping;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

public class ConfrMessageDecoderTest {
    @Test
    public void testDecodeSuccess() {
        EmbeddedChannel channel = new EmbeddedChannel(new ConfrMessageDecoder());

        Ping ping = new Ping((short) 1, 1, "incoming");
        ByteBuf buffer = Unpooled.buffer();
        ping.writeTo(buffer);
        channel.writeInbound(buffer);
        channel.flush();
        channel.finish();
        Object msgRead = channel.readInbound();
        Assert.assertTrue(msgRead instanceof Ping);
    }

    @Test
    public void testDecodeFailure() {
        EmbeddedChannel channel = new EmbeddedChannel(new ConfrMessageDecoder());
        ByteBuf buf = Unpooled.buffer().writeInt(-1);
        channel.writeInbound(buf);
        channel.flush();
        channel.finish();

        Assert.assertTrue(!channel.isOpen());
    }
}
