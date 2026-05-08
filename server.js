import { WebSocketServer, WebSocket } from 'ws';

const PORT = process.env.PORT || 3000;

// إنشاء WebSocket Server حقيقي
const wss = new WebSocketServer({
    port: PORT,
    perMessageDeflate: false
});

console.log(`[PROXY] Sentinel Nexus Engine V3 running on port ${PORT}`);

// Helper: تحقق من صلاحية الـ Stream
function isValidStream(stream) {
    return /^[a-z0-9@_]+$/i.test(stream);
}

wss.on('connection', (ws, req) => {

    // استخراج stream من الرابط
    const url = new URL(req.url, `http://${req.headers.host}`);
    const stream = url.searchParams.get('stream');

    // حماية من stream غير صالح
    if (!stream || !isValidStream(stream)) {
        console.log('[PROXY] Rejected invalid stream');
        ws.close(1008, 'Invalid stream');
        return;
    }

    const normalizedStream = stream.toLowerCase();

    console.log(`[CLIENT] Connected -> ${normalizedStream}`);

    let binanceWs = null;
    let reconnectTimer = null;
    let clientClosed = false;

    // =========================
    // CLIENT HEARTBEAT
    // =========================

    let clientAlive = true;

    ws.on('pong', () => {
        clientAlive = true;
    });

    const heartbeatInterval = setInterval(() => {

        if (clientClosed) return;

        if (!clientAlive) {
            console.log(`[CLIENT] Heartbeat timeout -> ${normalizedStream}`);
            ws.terminate();
            return;
        }

        clientAlive = false;

        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        }

    }, 30000);

    // =========================
    // CONNECT TO BINANCE
    // =========================

    const connectToBinance = () => {

        if (clientClosed) return;

        const binanceUrl =
            `wss://fstream.binance.com/ws/${normalizedStream}`;

        console.log(`[BINANCE] Connecting -> ${binanceUrl}`);

        binanceWs = new WebSocket(binanceUrl, {
            perMessageDeflate: false
        });

        // =========================
        // BINANCE OPEN
        // =========================

        binanceWs.on('open', () => {
            console.log(
                `[BINANCE] Connected -> ${normalizedStream}`
            );
        });

        // =========================
        // BINANCE MESSAGE
        // =========================

        binanceWs.on('message', (data) => {

            if (clientClosed) return;

            if (ws.readyState === WebSocket.OPEN) {

                // إرسال البيانات كما هي
                ws.send(data);

            }

        });

        // =========================
        // BINANCE PONG
        // =========================

        binanceWs.on('pong', () => {
            // اختياري للـ diagnostics
            // console.log(`[BINANCE] Pong -> ${normalizedStream}`);
        });

        // =========================
        // BINANCE ERROR
        // =========================

        binanceWs.on('error', (err) => {

            console.error(
                `[BINANCE ERROR] ${normalizedStream}:`,
                err.message
            );

            try {
                binanceWs.terminate();
            } catch {}

        });

        // =========================
        // BINANCE CLOSE
        // =========================

        binanceWs.on('close', (code, reason) => {

            console.log(
                `[BINANCE] Closed -> ${normalizedStream} | CODE=${code} | REASON=${reason}`
            );

            if (clientClosed) return;

            // منع duplicate reconnects
            if (reconnectTimer) return;

            console.log(
                `[BINANCE] Reconnecting in 3s -> ${normalizedStream}`
            );

            reconnectTimer = setTimeout(() => {

                reconnectTimer = null;

                if (!clientClosed) {
                    connectToBinance();
                }

            }, 3000);

        });

    };

    // بدء الاتصال الأول
    connectToBinance();

    // =========================
    // CLIENT -> BINANCE
    // =========================

    ws.on('message', (message) => {

        if (
            binanceWs &&
            binanceWs.readyState === WebSocket.OPEN
        ) {
            binanceWs.send(message);
        }

    });

    // =========================
    // CLIENT CLOSE
    // =========================

    ws.on('close', () => {

        console.log(
            `[CLIENT] Closed -> ${normalizedStream}`
        );

        clientClosed = true;

        clearInterval(heartbeatInterval);

        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }

        try {
            if (binanceWs) {
                binanceWs.terminate();
            }
        } catch {}

    });

    // =========================
    // CLIENT ERROR
    // =========================

    ws.on('error', (err) => {

        console.error(
            `[CLIENT ERROR] ${normalizedStream}:`,
            err.message
        );

    });

});
