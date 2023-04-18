import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.*
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.*
import io.netty.handler.codec.http2.*
import io.netty.util.AsciiString
import io.netty.util.ReferenceCountUtil
import io.netty.util.internal.ObjectUtil
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListMap


fun main() {
    startTestHttp2Server(9093, ObjectMapper().apply { registerKotlinModule() })
}

fun startTestHttp2Server(port: Int, objectMapper: ObjectMapper): ChannelFuture {
    val numberOfThreads = Runtime.getRuntime().availableProcessors() * 2
    val group: EventLoopGroup = NioEventLoopGroup(numberOfThreads)
    val b = ServerBootstrap()
    b.option(ChannelOption.SO_BACKLOG, 1024)
    b.group(group).channel(NioServerSocketChannel::class.java)
        .childHandler(Http2ServerInitializer(objectMapper))
    val ch = b.bind(port).sync().channel()
    return ch.closeFuture()
}


class Http2Handler internal constructor(
    decoder: Http2ConnectionDecoder,
    encoder: Http2ConnectionEncoder,
    initialSettings: Http2Settings,
    objectMapper: ObjectMapper
) : Http2ConnectionHandler(decoder, encoder, initialSettings), Http2FrameListener {

    private val streamIdToHeaders = ConcurrentSkipListMap<Int, RequestContext>()
    private val log = LoggerFactory.getLogger(Http2Handler::class.java)


    private fun sendResponse(
        ctx: ChannelHandlerContext,
        streamId: Int,
        payload: ByteBuf,
        status: HttpResponseStatus = HttpResponseStatus.OK
    ) {
        val headers = DefaultHttp2Headers().status(status.codeAsText())
        encoder().writeHeaders(ctx, streamId, headers, 0, false, ctx.newPromise())
        encoder().writeData(ctx, streamId, payload, 0, true, ctx.newPromise())
        ctx.flush()
    }

    private val streamIdToData = ConcurrentSkipListMap<Int, ByteBuf>()

    override fun onDataRead(
        ctx: ChannelHandlerContext, streamId: Int, data: ByteBuf, padding: Int, endOfStream: Boolean
    ): Int {
        val readableBytes = data.readableBytes()
        val processed = readableBytes + padding

        val buf = streamIdToData.computeIfAbsent(streamId) { _ -> ctx.alloc().compositeBuffer() }
        buf.writeBytes(data)

        if (endOfStream) {
            val accumulatedData = streamIdToData.remove(streamId)
            val byteBufInputStream: InputStream = ByteBufInputStream(accumulatedData, true)
            byteBufInputStream.close()

            val requestContext = streamIdToHeaders.remove(streamId)
                ?: throw IllegalStateException("Request context not found for streamId $streamId")

            val graphResponseFuture =
                CompletableFuture.completedFuture((mapOf<String, Any?>("data" to mapOf("userAgent" to requestContext.userAgent)) as GraphResponse))

            graphResponseFuture.whenCompleteAsync({ graphResponse, throwable ->
                if (throwable != null) {
                    log.error("Error processing data", throwable)
                    val errorPayload = ctx.alloc().buffer().writeBytes("Internal server error".toByteArray())
                    sendResponse(ctx, streamId, errorPayload, HttpResponseStatus.INTERNAL_SERVER_ERROR)
                } else {
                    val responseBuffer = ctx.alloc().buffer()
                    serializeGraphResponseToByteBuf(graphResponse, responseBuffer)
                    sendResponse(ctx, streamId, responseBuffer)
                }
            }, ctx.channel().eventLoop())
        }
        return processed
    }

    private val graphResponseWriter: ObjectWriter = objectMapper.writerFor(MutableMap::class.java)

    private fun serializeGraphResponseToByteBuf(graphResponse: GraphResponse, buffer: ByteBuf) {
        ByteBufOutputStream(buffer).use { outputStream: OutputStream ->
            graphResponseWriter.writeValue(outputStream, graphResponse)
        }
    }

    data class RequestContext(
        val userAgent: String?,
    )

    override fun onHeadersRead(
        ctx: ChannelHandlerContext, streamId: Int, headers: Http2Headers, padding: Int, endOfStream: Boolean
    ) {
        val method = headers.method()?.toString()
        val userAgent = headers.get("user-agent")?.toString()

        log.info("onHeadersRead: method=$method, userAgent=$userAgent")
        if (method == "POST") {
            streamIdToHeaders[streamId] = RequestContext(
                userAgent,
            )
        }
        if (method == "GET") {
            val path = headers.path()
            if (path.startsWith("/graphql")) {
                val graphResponseFuture =
                    CompletableFuture.supplyAsync { ((mapOf<String, Any?>("data" to mapOf("hello" to "world")) as GraphResponse)) }

                graphResponseFuture.whenCompleteAsync({ graphResponse, throwable ->
                    if (throwable != null) {
                        log.error("Error processing data", throwable)
                        val errorPayload = ctx.alloc().buffer().writeBytes("Internal server error".toByteArray())
                        sendResponse(ctx, streamId, errorPayload, HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    } else {
                        val responseBuffer = ctx.alloc().buffer()
                        serializeGraphResponseToByteBuf(graphResponse, responseBuffer)
                        sendResponse(ctx, streamId, responseBuffer)
                    }
                }, ctx.channel().eventLoop())
            } else {
                val payload = ctx.alloc().buffer().writeBytes("Hello world".toByteArray())
                sendResponse(ctx, streamId, payload, HttpResponseStatus.OK)
            }
        }
    }

    override fun onHeadersRead(
        ctx: ChannelHandlerContext,
        streamId: Int,
        headers: Http2Headers,
        streamDependency: Int,
        weight: Short,
        exclusive: Boolean,
        padding: Int,
        endOfStream: Boolean
    ) {
        onHeadersRead(ctx, streamId, headers, padding, endOfStream)
    }

    override fun onPriorityRead(
        ctx: ChannelHandlerContext, streamId: Int, streamDependency: Int, weight: Short, exclusive: Boolean
    ) {
    }

    override fun onRstStreamRead(ctx: ChannelHandlerContext, streamId: Int, errorCode: Long) {}
    override fun onSettingsAckRead(ctx: ChannelHandlerContext) {}
    override fun onSettingsRead(ctx: ChannelHandlerContext, settings: Http2Settings) {}
    override fun onPingRead(ctx: ChannelHandlerContext, data: Long) {}
    override fun onPingAckRead(ctx: ChannelHandlerContext, data: Long) {}
    override fun onPushPromiseRead(
        ctx: ChannelHandlerContext, streamId: Int, promisedStreamId: Int, headers: Http2Headers, padding: Int
    ) {
    }

    override fun onGoAwayRead(ctx: ChannelHandlerContext, lastStreamId: Int, errorCode: Long, debugData: ByteBuf) {}
    override fun onWindowUpdateRead(ctx: ChannelHandlerContext, streamId: Int, windowSizeIncrement: Int) {}
    override fun onUnknownFrame(
        ctx: ChannelHandlerContext, frameType: Byte, streamId: Int, flags: Http2Flags, payload: ByteBuf
    ) {
    }

    private fun http1HeadersToHttp2Headers(request: FullHttpRequest): Http2Headers? {
        val host: CharSequence? = request.headers()[HttpHeaderNames.HOST]
        val http2Headers = DefaultHttp2Headers()
            .method(HttpMethod.GET.asciiName())
            .path(request.uri())
            .scheme(HttpScheme.HTTP.name())
        if (host != null) {
            http2Headers.authority(host)
        }
        return http2Headers
    }

    @Throws(java.lang.Exception::class)
    override fun userEventTriggered(ctx: ChannelHandlerContext?, evt: Any?) {
        if (evt is HttpServerUpgradeHandler.UpgradeEvent) {
            onHeadersRead(ctx!!, 1, http1HeadersToHttp2Headers(evt.upgradeRequest())!!, 0, true)
        }
        super.userEventTriggered(ctx, evt)
    }

    override fun onError(ctx: ChannelHandlerContext?, outbound: Boolean, cause: Throwable?) {
        log.error("Error in http2 handler", cause)
        super.onError(ctx, outbound, cause)
    }
}

class Http2ServerInitializer(private val objectMapper: ObjectMapper) :
    ChannelInitializer<SocketChannel>() {

    private val upgradeCodecFactory =
        HttpServerUpgradeHandler.UpgradeCodecFactory { protocol ->
            if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
                Http2ServerUpgradeCodec(Http2HandlerBuilder(objectMapper).build())
            } else {
                null
            }
        }

    public override fun initChannel(ch: SocketChannel) {
        val p = ch.pipeline()
        val sourceCodec = HttpServerCodec()
        val upgradeHandler = HttpServerUpgradeHandler(sourceCodec, upgradeCodecFactory)
        val cleartextHttp2ServerUpgradeHandler = CleartextHttp2ServerUpgradeHandler(
            sourceCodec, upgradeHandler,
            Http2HandlerBuilder(objectMapper).build()
        )

        p.addLast(cleartextHttp2ServerUpgradeHandler)
        p.addLast(object : SimpleChannelInboundHandler<HttpMessage>() {
            @Throws(java.lang.Exception::class)
            override fun channelRead0(ctx: ChannelHandlerContext, msg: HttpMessage) {
                val pipeline = ctx.pipeline()
                pipeline.addAfter(ctx.name(), null, Http1Handler("Direct. No Upgrade Attempted."))
                pipeline.replace(this, null, HttpObjectAggregator(16 * 1024))
                ctx.fireChannelRead(ReferenceCountUtil.retain(msg))
            }
        })
    }
}

class Http2HandlerBuilder(private val objectMapper: ObjectMapper) :
    AbstractHttp2ConnectionHandlerBuilder<Http2Handler, Http2HandlerBuilder>() {

    init {
        initialSettings(
            Http2Settings()
                .maxConcurrentStreams(10000)
        )
    }

    public override fun build(): Http2Handler {
        return super.build()
    }

    override fun build(
        decoder: Http2ConnectionDecoder, encoder: Http2ConnectionEncoder, initialSettings: Http2Settings,
    ): Http2Handler {
        val handler = Http2Handler(decoder, encoder, initialSettings, objectMapper)
        frameListener(handler)
        return handler
    }

}

class Http1Handler(establishApproach: String) : SimpleChannelInboundHandler<FullHttpRequest>() {
    private val establishApproach: String

    init {
        this.establishApproach = ObjectUtil.checkNotNull(establishApproach, "establishApproach")
    }

    @Throws(java.lang.Exception::class)
    public override fun channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest) {
        if (HttpUtil.is100ContinueExpected(req)) {
            ctx.write(DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER))
        }
        val content = ctx.alloc().buffer()
        content.writeBytes("You must use HTTP/2 to access this resource".toByteArray())
        val response: FullHttpResponse =
            DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UPGRADE_REQUIRED, content)
        response.headers()[HttpHeaderNames.CONTENT_TYPE] = "text/plain; charset=UTF-8"
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
        response.headers()[HttpHeaderNames.UPGRADE] = "h2c"
        response.headers()[HttpHeaderNames.CONNECTION] = HttpHeaderValues.UPGRADE
        ctx.write(response).addListener(ChannelFutureListener.CLOSE)

    }

    @Throws(java.lang.Exception::class)
    override fun channelReadComplete(ctx: ChannelHandlerContext) {
        ctx.flush()
    }

}

typealias GraphResponse = MutableMap<String, Any>