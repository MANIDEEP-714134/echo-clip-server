        require("dotenv").config();
        const net = require("net");
        const http = require("http");
        const dgram = require("dgram");
        const fs = require("fs");
        const path = require("path");
        const crypto = require("crypto");
        const {
            execFile
        } = require("child_process");
        const { createClient } = require("redis");
        // =====================================================
        // CONFIGURATION
        // =====================================================

        const TCP_HOST = "0.0.0.0";
        const TCP_PORT = 5000;

        const HTTP_HOST = "0.0.0.0";
        const HTTP_PORT = 3000;

        // UDP audio transport (ESP32 real-time PCM)
        const UDP_HOST = "0.0.0.0";
        const UDP_PORT = 5001;
        const ECHO_UDP_MAGIC = 0x4543484F;
        const ECHO_UDP_VERSION = 1;
        const UDP_PACKET_START = 1;
        const UDP_PACKET_AUDIO = 2;
        const UDP_PACKET_END = 3;
        const UDP_DEVICE_ID_LEN = 24;
        const UDP_MIN_HEADER_SIZE = 46;
        const UDP_AUTH_TOKEN_LEN = 16;
        const UDP_SHARED_TOKEN = (process.env.UDP_SHARED_TOKEN || "").trim();
        const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

        const SAMPLE_RATE = 16000;
        const CHANNELS = 1;
        const BITS_PER_SAMPLE = 16;

        // =====================================================
        // LIVE TRANSCRIPTION
        // =====================================================

        const TRANSCRIPTION_CHUNK_SECONDS = 30;

        const TRANSCRIPTION_CHUNK_BYTES =
            SAMPLE_RATE *
            CHANNELS *
            (BITS_PER_SAMPLE / 8) *
            TRANSCRIPTION_CHUNK_SECONDS;

        const ELEVENLABS_API_KEY =
            process.env.ELEVENLABS_API_KEY;


        const ELEVENLABS_MODEL =
            "scribe_v2";

        // =====================================================
        // TRANSCRIPTION CACHE
        // =====================================================

        // 500 MB total rolling transcription cache across ALL devices.
        const TRANSCRIPT_CACHE_MAX_BYTES =
            500 * 1024 * 1024;

        // Set REDIS_URL in .env to your AWS ElastiCache/Valkey endpoint.
        // Use rediss:// when TLS is enabled.
        const REDIS_URL =
            process.env.REDIS_URL || "";

        const redisClient =
            REDIS_URL
                ? createClient({ url: REDIS_URL })
                : null;

        let redisReady = false;
        let transcriptCacheBytes = 0;
        let cacheMutationQueue = Promise.resolve();

        const TRANSCRIPT_GLOBAL_INDEX =
            "echoclip:transcript:index";

        const TRANSCRIPT_BYTES_KEY =
            "echoclip:transcript:bytes";

        function transcriptDeviceIndexKey(deviceId) {
            return `echoclip:transcript:device:${deviceId}`;
        }

        function transcriptChunkKey(deviceId, chunkId) {
            return `echoclip:transcript:chunk:${deviceId}:${chunkId}`;
        }

        function queueCacheMutation(fn) {
            const run =
                cacheMutationQueue.then(fn, fn);

            cacheMutationQueue =
                run.catch(() => {});

            return run;
        }

        // -----------------------------------------------------
        // IN-MEMORY FALLBACK
        // -----------------------------------------------------

        const memoryTranscriptCache =
            new Map();

        let memoryTranscriptCacheBytes = 0;

        // -----------------------------------------------------
        // REDIS / ELASTICACHE INITIALIZATION
        // -----------------------------------------------------

        async function initializeTranscriptCache() {

            if (!redisClient) {

                console.log(
                    "Transcript cache: IN-MEMORY FALLBACK"
                );

                return;
            }

            redisClient.on(
                "error",
                error => {

                    redisReady = false;

                    console.error(
                        "Redis cache error:",
                        error.message
                    );
                }
            );

            try {

                await redisClient.connect();

                redisReady = true;

                const storedBytes =
                    await redisClient.get(
                        TRANSCRIPT_BYTES_KEY
                    );

                transcriptCacheBytes =
                    Number(storedBytes || 0);

                console.log(
                    `Transcript cache: REDIS/ELASTICACHE CONNECTED | ` +
                    `limit 500 MB | ` +
                    `current ${(transcriptCacheBytes / 1024 / 1024).toFixed(2)} MB`
                );

                await evictTranscriptCacheIfNeeded();

            }
            catch (error) {

                redisReady = false;

                console.error(
                    "Redis connection failed:",
                    error.message
                );

                console.error(
                    "Using bounded in-memory transcription cache."
                );
            }
        }

        function getCacheMode() {

            return redisReady
                ? "redis"
                : "memory";
        }

        // -----------------------------------------------------
        // EVICT OLDEST DATA
        // -----------------------------------------------------

        async function evictTranscriptCacheIfNeeded() {

            if (!redisReady) {

                while (
                    memoryTranscriptCacheBytes >
                        TRANSCRIPT_CACHE_MAX_BYTES &&
                    memoryTranscriptCache.size > 0
                ) {

                    const oldestKey =
                        memoryTranscriptCache
                            .keys()
                            .next()
                            .value;

                    const entry =
                        memoryTranscriptCache.get(
                            oldestKey
                        );

                    memoryTranscriptCache.delete(
                        oldestKey
                    );

                    memoryTranscriptCacheBytes =
                        Math.max(
                            0,
                            memoryTranscriptCacheBytes -
                                Number(entry.bytes || 0)
                        );
                }

                return;
            }

            while (
                transcriptCacheBytes >
                    TRANSCRIPT_CACHE_MAX_BYTES
            ) {

                const oldest =
                    await redisClient.zRange(
                        TRANSCRIPT_GLOBAL_INDEX,
                        0,
                        0
                    );

                if (!oldest.length) {

                    transcriptCacheBytes = 0;

                    await redisClient.set(
                        TRANSCRIPT_BYTES_KEY,
                        "0"
                    );

                    break;
                }

                const chunkKey =
                    oldest[0];

                const raw =
                    await redisClient.get(
                        chunkKey
                    );

                await redisClient.zRem(
                    TRANSCRIPT_GLOBAL_INDEX,
                    chunkKey
                );

                if (!raw) {
                    continue;
                }

                let entry;

                try {
                    entry = JSON.parse(raw);
                }
                catch (_) {
                    entry = {
                        bytes:
                            Buffer.byteLength(
                                raw,
                                "utf8"
                            )
                    };
                }

                const bytes =
                    Number(entry.bytes || 0);

                if (entry.deviceId) {

                    await redisClient.zRem(
                        transcriptDeviceIndexKey(
                            entry.deviceId
                        ),
                        chunkKey
                    );
                }

                await redisClient.del(
                    chunkKey
                );

                transcriptCacheBytes =
                    Math.max(
                        0,
                        transcriptCacheBytes -
                            bytes
                    );

                await redisClient.set(
                    TRANSCRIPT_BYTES_KEY,
                    String(transcriptCacheBytes)
                );
            }
        }

        // -----------------------------------------------------
        // ADD TRANSCRIPT
        // -----------------------------------------------------

        async function addTranscriptToCache(
            device,
            chunkNumber,
            text
        ) {

            const normalizedText =
                String(text || "").trim();

            if (!normalizedText) {
                return;
            }

            const entry = {
                deviceId: device.id,
                chunkNumber,
                timestamp: Date.now(),
                text: normalizedText,
                bytes:
                    Buffer.byteLength(
                        normalizedText,
                        "utf8"
                    )
            };

            if (
                entry.bytes >
                    TRANSCRIPT_CACHE_MAX_BYTES
            ) {

                console.warn(
                    `[${device.id}] Transcript chunk is larger than 500 MB. Discarded.`
                );

                return;
            }

            await queueCacheMutation(
                async () => {

                    if (!redisReady) {

                        const chunkId =
                            `${Date.now()}-${crypto
                                .randomBytes(6)
                                .toString("hex")}`;

                        memoryTranscriptCache.set(
                            chunkId,
                            entry
                        );

                        memoryTranscriptCacheBytes +=
                            entry.bytes;

                        await evictTranscriptCacheIfNeeded();

                        return;
                    }

                    const chunkId =
                        `${Date.now()}-${crypto
                            .randomBytes(6)
                            .toString("hex")}`;

                    const key =
                        transcriptChunkKey(
                            device.id,
                            chunkId
                        );

                    await redisClient.set(
                        key,
                        JSON.stringify(entry)
                    );

                    await redisClient.zAdd(
                        TRANSCRIPT_GLOBAL_INDEX,
                        {
                            score: entry.timestamp,
                            value: key
                        }
                    );

                    await redisClient.zAdd(
                        transcriptDeviceIndexKey(
                            device.id
                        ),
                        {
                            score: entry.timestamp,
                            value: key
                        }
                    );

                    transcriptCacheBytes +=
                        entry.bytes;

                    await redisClient.set(
                        TRANSCRIPT_BYTES_KEY,
                        String(transcriptCacheBytes)
                    );

                    await evictTranscriptCacheIfNeeded();
                }
            );
        }

        // -----------------------------------------------------
        // GET DEVICE TRANSCRIPT
        // -----------------------------------------------------

        async function getDeviceTranscript(
            deviceId
        ) {

            if (!redisReady) {

                return Array.from(
                    memoryTranscriptCache.values()
                )
                    .filter(
                        entry =>
                            entry.deviceId === deviceId
                    )
                    .sort(
                        (a, b) => {

                            if (
                                a.chunkNumber !==
                                b.chunkNumber
                            ) {
                                return (
                                    a.chunkNumber -
                                    b.chunkNumber
                                );
                            }

                            return (
                                a.timestamp -
                                b.timestamp
                            );
                        }
                    );
            }

            const keys =
                await redisClient.zRange(
                    transcriptDeviceIndexKey(
                        deviceId
                    ),
                    0,
                    -1
                );

            if (!keys.length) {
                return [];
            }

            const values =
                await redisClient.mGet(
                    keys
                );

            return values
                .filter(Boolean)
                .map(raw => {

                    try {
                        return JSON.parse(raw);
                    }
                    catch (_) {
                        return null;
                    }
                })
                .filter(Boolean)
                .sort(
                    (a, b) => {

                        if (
                            a.chunkNumber !==
                            b.chunkNumber
                        ) {
                            return (
                                a.chunkNumber -
                                b.chunkNumber
                            );
                        }

                        return (
                            a.timestamp -
                            b.timestamp
                        );
                    }
                );
        }

        // -----------------------------------------------------
        // CACHE STATUS
        // -----------------------------------------------------

        async function getTranscriptCacheStats() {

            const bytes =
                redisReady
                    ? Number(
                        await redisClient.get(
                            TRANSCRIPT_BYTES_KEY
                        ) || 0
                    )
                    : memoryTranscriptCacheBytes;

            return {
                backend:
                    getCacheMode(),

                maxBytes:
                    TRANSCRIPT_CACHE_MAX_BYTES,

                maxMB:
                    TRANSCRIPT_CACHE_MAX_BYTES /
                    1024 / 1024,

                usedBytes:
                    bytes,

                usedMB:
                    Number(
                        (
                            bytes /
                            1024 /
                            1024
                        ).toFixed(2)
                    ),

                usagePercent:
                    Number(
                        (
                            bytes /
                            TRANSCRIPT_CACHE_MAX_BYTES *
                            100
                        ).toFixed(2)
                    )
            };
        }


        if (!ELEVENLABS_API_KEY) {

            console.error(
                "ERROR: ELEVENLABS_API_KEY is not configured."
            );

        }

        // =====================================================
        // DIRECTORIES
        // =====================================================

        // Audio is NEVER stored permanently.
        // Temporary WAV files are kept only during transcription.
        const os = require("os");

        const tempAudioDir =
            path.join(
                os.tmpdir(),
                "echoclip-transcription"
            );

        if (!fs.existsSync(tempAudioDir)) {
            fs.mkdirSync(
                tempAudioDir,
                { recursive: true }
            );
        }


        // =====================================================
        // DEVICE DATABASE
        // =====================================================

        const devices = new Map();


        // =====================================================
        // TIMESTAMP
        // =====================================================

        // =====================================================
        // RUN PYTHON AUDIO PROCESSOR
        // =====================================================

        function runAudioProcessor(
            inputWav,
            outputWav
        ) {

            return new Promise(
                (
                    resolve,
                    reject
                ) => {

                    const pythonCommand =
                        process.platform === "win32"
                            ? "python"
                            : "python3";


                    const scriptPath =
                        path.join(
                            __dirname,
                            "audio_processor.py"
                        );


                    execFile(
                        pythonCommand,

                        [
                            scriptPath,
                            inputWav,
                            outputWav
                        ],

                        {
                            maxBuffer:
                                10 * 1024 * 1024
                        },

                        (
                            error,
                            stdout,
                            stderr
                        ) => {

                            if (stdout) {

                                console.log(
                                    stdout
                                );
                            }


                            if (stderr) {

                                console.log(
                                    stderr
                                );
                            }


                            if (error) {

                                reject(
                                    error
                                );

                                return;
                            }


                            resolve();
                        }
                    );
                }
            );
        }

        function getTimestamp() {

            const now = new Date();

            const pad = (n) =>
                String(n).padStart(2, "0");

            return (
                now.getFullYear() +
                pad(now.getMonth() + 1) +
                pad(now.getDate()) +
                "_" +
                pad(now.getHours()) +
                pad(now.getMinutes()) +
                pad(now.getSeconds())
            );
        }


        // =====================================================
        // CREATE WAV
        // =====================================================

        // =====================================================
        // DEVICE ID
        // =====================================================

        function generateDeviceId() {

            return (
                "SERVER-" +
                crypto
                    .randomBytes(4)
                    .toString("hex")
                    .toUpperCase()
            );
        }


        // =====================================================
        // CREATE DEVICE OBJECT
        // =====================================================

        function createDevice(
            deviceId,
            socket
        ) {

            return {

                id: deviceId,

                socket: socket,

                ip:
                    socket.remoteAddress,

                connected:
                    true,

                recording:
                    false,

                lastSeen:
                    Date.now(),

                connectedAt:
                    Date.now(),

                lastCommand:
                    null,

                lastCommandTime:
                    null,

                audioBytes:
                    0,

                recordingBytes:
                    0,

                recordingStarted:
                    null,

                recordingId:
                    null,

                expectingRecordingData:
                    false,

                transcriptionBuffer:
                    [],

                transcriptionBufferBytes:
                    0,

                transcriptionChunkNumber:
                    0,

                transcriptionProcessing:
                    false,

                liveTranscript:
                    ""
            };
        }


        // =====================================================
        // SEND COMMAND TO DEVICE
        // =====================================================

        function sendCommand(
            device,
            command
        ) {

            if (
                !device ||
                !device.socket ||
                !device.connected
            ) {
                return false;
            }


            try {

                device.socket.write(
                    command.trim() + "\n"
                );


                device.lastCommand =
                    command;

                device.lastCommandTime =
                    Date.now();


                console.log(
                    `[${device.id}] → ${command}`
                );


                return true;

            }
            catch (error) {

                console.error(
                    "Command error:",
                    error.message
                );


                return false;
            }
        }


        // =====================================================
        // START RECORDING
        // =====================================================

        function startRecording(
            device
        ) {

            if (!device.connected) {

                return {
                    success: false,
                    error: "DEVICE_OFFLINE"
                };
            }


            if (device.recording) {

                return {
                    success: true,
                    message: "ALREADY_RECORDING"
                };
            }


            // -------------------------------------------------
            // RECORDING SESSION
            // -------------------------------------------------
            // No permanent PCM/WAV file is created.
            // Audio remains in RAM only until a 30-second
            // transcription chunk is processed.

            const timestamp =
                getTimestamp();

            const recordingId =
                `${device.id}_${timestamp}`;

            device.recordingId =
                recordingId;

            device.recordingBytes =
                0;

            device.transcriptionBuffer =
                [];

            device.transcriptionBufferBytes =
                0;

            device.transcriptionChunkNumber =
                0;

            device.transcriptionProcessing =
                false;

            device.liveTranscript =
                "";

            device.expectingRecordingData =
                false;

            device.recordingStarted =
                Date.now();

            device.recording =
                true;

            // -------------------------------------------------
            // SEND COMMAND
            // -------------------------------------------------

            const sent =
                sendCommand(
                    device,
                    "START"
                );


            if (!sent) {
                device.recording = false;

                return {
                    success: false,
                    error: "COMMAND_FAILED"
                };
            }


            console.log(
                `[${device.id}] Recording started`
            );


            return {
                success: true,

                recordingId:
                    recordingId,

                message:
                    "RECORDING_STARTED"
            };
        }


        // =====================================================
        // STOP RECORDING
        // =====================================================

        function stopRecording(
            device
        ) {

            if (!device) {

                return {
                    success: false,
                    error: "DEVICE_NOT_FOUND"
                };
            }


            if (!device.recording) {

                // Still tell ESP32 to stop

                if (device.connected) {

                    sendCommand(
                        device,
                        "STOP"
                    );
                }


                return {
                    success: true,
                    message: "NOT_RECORDING"
                };
            }


            // -------------------------------------------------
            // SEND STOP
            // -------------------------------------------------

            if (device.connected) {

                sendCommand(
                    device,
                    "STOP"
                );
            }


            device.recording =
            false;


        // -------------------------------------------------
        // PROCESS FINAL PARTIAL TRANSCRIPTION CHUNK
        // -------------------------------------------------

        processRemainingTranscription(
            device
        )
        .catch(
            error => {
                console.error(
                    `[${device.id}] Final transcription failed:`,
                    error.message
                );
            }
        );


        // -------------------------------------------------
        // CLOSE FILE
        // -------------------------------------------------

        finishRecording(
            device
        );


            return {
                success: true,

                recordingId:
                    device.recordingId,

                message:
                    "RECORDING_STOPPED"
            };
        }


        // =====================================================
        // FINISH RECORDING
        // =====================================================

        function finishRecording(
            device
        ) {

            if (!device) {
                return;
            }

            // Permanent recording storage is disabled.
            // Temporary transcription files are removed by
            // processTranscriptionChunk().

            device.recording =
                false;
        }


        // =====================================================
        // FACTORY RESET
        // =====================================================

        function factoryReset(
            device
        ) {

            if (!device) {

                return {
                    success: false,
                    error: "DEVICE_NOT_FOUND"
                };
            }


            if (!device.connected) {

                return {
                    success: false,
                    error: "DEVICE_OFFLINE"
                };
            }


            if (device.recording) {

                stopRecording(
                    device
                );
            }


            const sent =
                sendCommand(
                    device,
                    "FACTORY_RESET"
                );


            if (!sent) {

                return {
                    success: false,
                    error: "COMMAND_FAILED"
                };
            }


            return {
                success: true,

                message:
                    "FACTORY_RESET_SENT"
            };
        }


        // =====================================================
        // GET DEVICE
        // =====================================================

        function getDevice(
            deviceId
        ) {

            return devices.get(
                deviceId
            );
        }


        // =====================================================
        // DEVICE JSON
        // =====================================================

        function deviceToJSON(
            device
        ) {

            if (!device) {

                return null;
            }


            let duration = 0;

            if (device.recordingBytes > 0) {

                duration =
                    device.recordingBytes /
                    (
                        SAMPLE_RATE *
                        CHANNELS *
                        (BITS_PER_SAMPLE / 8)
                    );

            }
            else if (
                device.recording &&
                device.recordingStarted
            ) {

                duration =
                    (
                        Date.now() -
                        device.recordingStarted
                    ) / 1000;
            }


            return {

                id:
                    device.id,

                ip:
                    device.ip,

                connected:
                    device.connected,

                recording:
                    device.recording,

                lastSeen:
                    new Date(
                        device.lastSeen
                    ).toISOString(),

                connectedAt:
                    new Date(
                        device.connectedAt
                    ).toISOString(),

                lastCommand:
                    device.lastCommand,

                recordingId:
                    device.recordingId,

                recordingBytes:
                    device.recordingBytes,

                recordingDuration:
                    Number(
                        duration.toFixed(2)
                    )
            };
        }


        // =====================================================
        // TCP SERVER
        // =====================================================

    const tcpServer =
        net.createServer(
            (socket) => {

                // =============================================
                // LOW-LATENCY TCP AUDIO SETTINGS
                // =============================================

                // Disable Nagle's algorithm.
                // Important for real-time audio packets.
                socket.setNoDelay(true);

                // Keep the TCP connection alive.
                socket.setKeepAlive(true, 10000);

                console.log("");
                    console.log(
                        "======================================"
                    );

                    console.log(
                        "ESP32 TCP CONNECTION"
                    );

                    console.log(
                        "TCP Remote IP:",
                        socket.remoteAddress
                    );

                    console.log(
                        "TCP Remote Port:",
                        socket.remotePort
                    );

                    console.log(
                        "======================================"
                    );


                    let device =
                        null;


                    let registered =
                        false;


                    let protocolBuffer =
                        Buffer.alloc(0);





                    // =================================================
                    // SOCKET DATA
                    // =================================================

                    socket.on(
                        "data",
                        (data) => {

                            // =================================================
                            // DEVICE NOT REGISTERED
                            // =================================================

                            if (!registered) {

                                protocolBuffer =
                                    Buffer.concat([
                                        protocolBuffer,
                                        data
                                    ]);

                                processHandshake();

                                return;
                            }


                            // =================================================
                            // DEVICE REGISTERED
                            // =================================================

                            device.lastSeen =
                                Date.now();


                            // =================================================
                            // RECORDING DATA
                            // =================================================

                            if (device.recording) {

                                // UDP-mode ESP32 sends this status marker over TCP.
                                // Audio itself arrives on UDP :5001, so never treat this marker as PCM.
                                const tcpText = data.toString("utf8");
                                if (tcpText.includes("RECORDING_UDP")) {
                                    device.expectingRecordingData = false;
                                    console.log(`[${device.id}] UDP audio mode armed`);
                                    return;
                                }

                                /*
                                * Everything received after START
                                * is audio data.
                                *
                                * The ESP32 may send a small
                                * "RECORDING\n" marker first.
                                */

                                if (!device.expectingRecordingData) {

                                    const marker =
                                        Buffer.from(
                                            "RECORDING\n"
                                        );


                                    const index =
                                        data.indexOf(
                                            marker
                                        );


                                    if (index !== -1) {

                                        console.log(
                                            `[${device.id}] Audio stream started`
                                        );


                                        device.expectingRecordingData = true;


                                        const audioStart =
                                            index +
                                            marker.length;


                                        const remaining =
                                            data.subarray(
                                                audioStart
                                            );


                                        if (
                                            remaining.length > 0
                                        ) {

                                            writeAudio(
                                                remaining
                                            );
                                        }


                                        return;
                                    }


                                    /*
                                    * If the ESP32 does not send
                                    * the marker, treat the data
                                    * directly as PCM.
                                    */

                                    device.expectingRecordingData =
                                        true;
                                }


                                // ---------------------------------------------
                                // RAW PCM
                                // ---------------------------------------------

                                writeAudio(
                                    data
                                );

                                return;
                            }


                            // =================================================
                            // NON-RECORDING DATA
                            // =================================================

                            /*
                            * Ignore anything else from the ESP32
                            * while not recording.
                            */

                        }
                    );



                    // =================================================
                    // HANDSHAKE
                    // =================================================

                    function processHandshake() {
                        const text =
                            protocolBuffer.toString();


                        const helloIndex =
                            text.indexOf(
                                "HELLO "
                            );


                        if (
                            helloIndex === -1
                        ) {
                            return;
                        }


                        const newline =
                            text.indexOf(
                                "\n",
                                helloIndex
                            );


                        if (
                            newline === -1
                        ) {
                            return;
                        }


                        const deviceId =
                            text
                                .substring(
                                    helloIndex + 6,
                                    newline
                                )
                                .trim();


                        if (
                            !deviceId
                        ) {
                            return;
                        }


                        // -----------------------------------------
                        // REGISTER
                        // -----------------------------------------

                        device =
                            devices.get(
                                deviceId
                            );


                        if (!device) {

                            device =
                                createDevice(
                                    deviceId,
                                    socket
                                );


                            devices.set(
                                deviceId,
                                device
                            );

                        }
                        else {

                            // Existing device reconnecting

                            if (
                                device.socket &&
                                device.socket !== socket
                            ) {

                                try {
                                    device.socket.destroy();
                                }
                                catch (_) { }
                            }


                            device.socket =
                                socket;

                            device.ip =
                                socket.remoteAddress;

                            device.connected =
                                true;

                            device.connectedAt =
                                Date.now();

                            device.lastSeen =
                                Date.now();
                        }


                        registered =
                            true;


                        protocolBuffer =
                            Buffer.alloc(0);


                        console.log(
                            `[${device.id}] REGISTERED`
                        );

                        console.log(
                            `[${device.id}] IP: ${device.ip}`
                        );


                        // -----------------------------------------
                        // CHECK READY
                        // -----------------------------------------

                        sendCommand(
                            device,
                            "PING"
                        );
                    }


                    // =================================================
                    // WRITE AUDIO
                    // =================================================

                    function writeAudio(
                        audioData
                    ) {

                        if (
                            !device ||
                            !device.recording
                        ) {
                            return;
                        }


                        if (
                            audioData.length === 0
                        ) {
                            return;
                        }


                        // Audio is held only in RAM for the
                        // active 30-second transcription chunk.
                        device.recordingBytes +=
                            audioData.length;


                        device.audioBytes +=
                            audioData.length;

                        // =================================================
                        // LIVE TRANSCRIPTION BUFFER
                        // =================================================

                        device.transcriptionBuffer.push(
                            Buffer.from(audioData)
                        );

                        device.transcriptionBufferBytes +=
                            audioData.length;

                        processTranscriptionBuffer(
                            device
                        );
                        // -----------------------------------------
                        // PROGRESS
                        // -----------------------------------------

                        if (
                            device.recordingBytes %
                            32000 <
                            audioData.length
                        ) {

                            const seconds =
                                device.recordingBytes /
                                (
                                    SAMPLE_RATE *
                                    CHANNELS *
                                    2
                                );


                            console.log(
                                `[${device.id}] ` +
                                `Recording ${seconds.toFixed(1)} sec | ` +
                                `${device.recordingBytes} bytes`
                            );
                        }
                    }


                    // =================================================
                    // END
                    // =================================================

                    socket.on(
                        "end",
                        () => {

                            handleDisconnect();
                        }
                    );


                    // =================================================
                    // CLOSE
                    // =================================================

                    socket.on(
                        "close",
                        () => {

                            handleDisconnect();
                        }
                    );


                    // =================================================
                    // ERROR
                    // =================================================

                    socket.on(
                        "error",
                        (error) => {

                            console.error(
                                `[${device?.id || "UNKNOWN"}] Socket error:`,
                                error.message
                            );
                        }
                    );


                    // =================================================
                    // DISCONNECT
                    // =================================================

                    function handleDisconnect() {
                        if (!device)
                            return;


                        if (
                            device.socket !== socket
                        ) {
                            return;
                        }


                        device.connected =
                            false;


                        device.lastSeen =
                            Date.now();


                        if (
                            device.recording
                        ) {

                            device.recording =
                                false;

                            processRemainingTranscription(
                                device
                            )
                            .catch(
                                error => {
                                    console.error(
                                        `[${device.id}] Final transcription on disconnect failed:`,
                                        error.message
                                    );
                                }
                            );

                            finishRecording(
                                device
                            );
                        }


                        console.log(
                            `[${device.id}] DISCONNECTED`
                        );
                    }

                }
            );


        // =====================================================
        // PRODUCTION UDP AUDIO SERVER (ESP32 -> AWS)
        // =====================================================

        const udpServer = dgram.createSocket("udp4");

        const UDP_REORDER_MAX_PACKETS = 4;
        const UDP_REORDER_MAX_WAIT_MS = 120;

        const udpStats = {
            packets: 0,
            audioPackets: 0,
            audioBytes: 0,
            malformed: 0,
            unknownDevice: 0,
            droppedSequence: 0,
            reorderedPackets: 0,
            duplicatePackets: 0,
            latePackets: 0,
            recoveredSilenceBytes: 0,
            activeStreams: 0
        };

        // Per-device jitter/reorder state. UDP is intentionally unordered,
        // so production audio must not immediately concatenate packets.
        const udpStreams = new Map();

        function getUdpStream(deviceId) {
            let stream = udpStreams.get(deviceId);

            if (!stream) {
                stream = {
                    nextSequence: null,
                    pending: new Map(),
                    firstPendingAt: 0,
                    payloadBytes: 0,
                    lastPacketAt: Date.now()
                };
                udpStreams.set(deviceId, stream);
                udpStats.activeStreams = udpStreams.size;
            }

            return stream;
        }

        function resetUdpStream(deviceId) {
            udpStreams.set(deviceId, {
                nextSequence: null,
                pending: new Map(),
                firstPendingAt: 0,
                payloadBytes: 0,
                lastPacketAt: Date.now()
            });
            udpStats.activeStreams = udpStreams.size;
            return udpStreams.get(deviceId);
        }

        function isSequenceBehind(sequence, expected) {
            if (expected === null || expected === undefined) return false;
            const distance = (sequence - expected) >>> 0;
            return distance > 0x80000000;
        }

        function parseEchoUdpPacket(message) {
            if (!Buffer.isBuffer(message) || message.length < UDP_MIN_HEADER_SIZE) {
                return null;
            }

            const magic = message.readUInt32LE(0);
            const version = message.readUInt8(4);
            const packetType = message.readUInt8(5);
            const headerSize = message.readUInt16LE(6);
            const sequence = message.readUInt32LE(8);
            const timestampMs = message.readUInt32LE(12);
            const payloadBytes = message.readUInt16LE(16);
            const sampleRate = message.readUInt16LE(18);
            const bitsPerSample = message.readUInt8(20);
            const channels = message.readUInt8(21);

            if (magic !== ECHO_UDP_MAGIC || version !== ECHO_UDP_VERSION) return null;
            if (headerSize < UDP_MIN_HEADER_SIZE || headerSize > message.length) return null;
            if (payloadBytes > 1400) return null;
            if (headerSize + payloadBytes > message.length) return null;

            const deviceId = message
                .subarray(22, 22 + UDP_DEVICE_ID_LEN)
                .toString("utf8")
                .replace(/\0.*$/, "")
                .trim();

            if (!deviceId) return null;

            let authToken = "";
            if (headerSize >= UDP_MIN_HEADER_SIZE + UDP_AUTH_TOKEN_LEN) {
                authToken = message
                    .subarray(46, 46 + UDP_AUTH_TOKEN_LEN)
                    .toString("utf8")
                    .replace(/\0.*$/, "")
                    .trim();
            }

            if (UDP_SHARED_TOKEN && authToken !== UDP_SHARED_TOKEN) {
                return null;
            }

            return {
                packetType,
                sequence,
                timestampMs,
                payloadBytes,
                sampleRate,
                bitsPerSample,
                channels,
                deviceId,
                payload: message.subarray(headerSize, headerSize + payloadBytes)
            };
        }

        function ingestUdpAudio(device, payload) {
            if (!device || !device.recording || !payload || payload.length === 0) return;

            device.lastSeen = Date.now();
            device.expectingRecordingData = true;
            device.recordingBytes += payload.length;
            device.audioBytes += payload.length;
            device.transcriptionBuffer.push(Buffer.from(payload));
            device.transcriptionBufferBytes += payload.length;
            processTranscriptionBuffer(device);

            if (device.recordingBytes % 32000 < payload.length) {
                const seconds = device.recordingBytes /
                    (SAMPLE_RATE * CHANNELS * (BITS_PER_SAMPLE / 8));
                console.log(
                    `[${device.id}] UDP Recording ${seconds.toFixed(1)} sec | ` +
                    `${device.recordingBytes} bytes`
                );
            }
        }

        function flushUdpStream(device, stream, force) {
            if (!device || !stream || stream.nextSequence === null) return;

            while (stream.pending.has(stream.nextSequence)) {
                const payload = stream.pending.get(stream.nextSequence);
                stream.pending.delete(stream.nextSequence);
                ingestUdpAudio(device, payload);
                stream.payloadBytes = payload.length;
                stream.nextSequence = (stream.nextSequence + 1) >>> 0;
            }

            if (stream.pending.size === 0) {
                stream.firstPendingAt = 0;
                return;
            }

            const waitedMs = stream.firstPendingAt
                ? Date.now() - stream.firstPendingAt
                : 0;

            // If the expected packet has not arrived within the jitter window,
            // preserve the audio timeline by inserting silence for the missing
            // packet rather than compressing the recording in time.
            while (
                stream.pending.size > UDP_REORDER_MAX_PACKETS ||
                (force && stream.pending.size > 0) ||
                (stream.firstPendingAt && waitedMs >= UDP_REORDER_MAX_WAIT_MS)
            ) {
                const silenceBytes = stream.payloadBytes ||
                    stream.pending.values().next().value?.length || 0;

                if (silenceBytes <= 0) break;

                ingestUdpAudio(
                    device,
                    Buffer.alloc(silenceBytes)
                );

                udpStats.droppedSequence++;
                udpStats.recoveredSilenceBytes += silenceBytes;
                stream.nextSequence = (stream.nextSequence + 1) >>> 0;

                while (stream.pending.has(stream.nextSequence)) {
                    const payload = stream.pending.get(stream.nextSequence);
                    stream.pending.delete(stream.nextSequence);
                    ingestUdpAudio(device, payload);
                    stream.payloadBytes = payload.length;
                    stream.nextSequence = (stream.nextSequence + 1) >>> 0;
                }

                if (stream.pending.size === 0) {
                    stream.firstPendingAt = 0;
                    break;
                }

                stream.firstPendingAt = Date.now();
            }
        }

        udpServer.on("message", (message, rinfo) => {
            udpStats.packets++;

            const packet = parseEchoUdpPacket(message);
            if (!packet) {
                udpStats.malformed++;
                return;
            }

            const device = devices.get(packet.deviceId);
            if (!device) {
                udpStats.unknownDevice++;
                if (udpStats.unknownDevice <= 5 || udpStats.unknownDevice % 100 === 0) {
                    console.warn(`[${packet.deviceId}] UDP packet from unregistered device ${rinfo.address}`);
                }
                return;
            }

            device.lastSeen = Date.now();

            if (packet.packetType === UDP_PACKET_START) {
                resetUdpStream(packet.deviceId);
                const stream = udpStreams.get(packet.deviceId);
                stream.nextSequence = packet.sequence >>> 0;
                console.log(`[${device.id}] UDP AUDIO START from ${rinfo.address}:${rinfo.port}`);
                return;
            }

            if (packet.packetType === UDP_PACKET_END) {
                const stream = udpStreams.get(packet.deviceId);
                if (stream) {
                    flushUdpStream(device, stream, true);
                }
                udpStreams.delete(packet.deviceId);
                udpStats.activeStreams = udpStreams.size;
                console.log(`[${device.id}] UDP AUDIO END`);
                return;
            }

            if (packet.packetType !== UDP_PACKET_AUDIO) return;
            if (!device.recording) return;

            if (
                packet.sampleRate !== SAMPLE_RATE ||
                packet.bitsPerSample !== BITS_PER_SAMPLE ||
                packet.channels !== CHANNELS
            ) {
                udpStats.malformed++;
                console.warn(
                    `[${device.id}] UDP audio format mismatch: ` +
                    `${packet.sampleRate}Hz/${packet.bitsPerSample}bit/${packet.channels}ch`
                );
                return;
            }

            const stream = getUdpStream(packet.deviceId);
            stream.lastPacketAt = Date.now();
            stream.payloadBytes = packet.payload.length;

            if (stream.nextSequence === null) {
                stream.nextSequence = packet.sequence >>> 0;
            }

            if (isSequenceBehind(packet.sequence, stream.nextSequence)) {
                udpStats.latePackets++;
                return;
            }

            if (stream.pending.has(packet.sequence)) {
                udpStats.duplicatePackets++;
                return;
            }

            if (packet.sequence !== stream.nextSequence) {
                udpStats.reorderedPackets++;
                if (stream.firstPendingAt === 0) {
                    stream.firstPendingAt = Date.now();
                }
            }

            stream.pending.set(packet.sequence, packet.payload);
            udpStats.audioPackets++;
            udpStats.audioBytes += packet.payload.length;

            flushUdpStream(device, stream, false);
        });

        // Periodically flush a packet that has waited beyond the jitter window.
        // This prevents a single lost packet from permanently stalling a stream.
        const udpFlushTimer = setInterval(() => {
            const now = Date.now();
            for (const [deviceId, stream] of udpStreams) {
                if (!stream.pending.size) continue;
                if (!stream.firstPendingAt) continue;
                if (now - stream.firstPendingAt < UDP_REORDER_MAX_WAIT_MS) continue;

                const device = devices.get(deviceId);
                if (device) flushUdpStream(device, stream, false);
            }
        }, 25);
        udpFlushTimer.unref?.();

        udpServer.on("error", error => {
            console.error("UDP SERVER ERROR:", error);
        });

        // =====================================================
        // TCP ERROR
        // =====================================================

        tcpServer.on(
            "error",
            (error) => {

                console.error(
                    "TCP SERVER ERROR:",
                    error
                );
            }
        );


        // =====================================================
        // HTTP HELPERS
        // =====================================================

        function sendJSON(
            res,
            statusCode,
            data
        ) {

            const body =
                JSON.stringify(
                    data,
                    null,
                    2
                );


            res.writeHead(
                statusCode,
                {
                    "Content-Type":
                        "application/json",

                    "Access-Control-Allow-Origin":
                        ALLOWED_ORIGIN,

                    "Access-Control-Allow-Methods":
                        "GET,POST,OPTIONS",

                    "Access-Control-Allow-Headers":
                        "Content-Type"
                }
            );


            res.end(
                body
            );
        }


        // =====================================================
        // READ HTTP BODY
        // =====================================================

        function readBody(
            req
        ) {

            return new Promise(
                (resolve, reject) => {

                    let body =
                        "";


                    req.on(
                        "data",
                        chunk => {

                            body +=
                                chunk.toString();

                            if (
                                body.length >
                                1000000
                            ) {

                                reject(
                                    new Error(
                                        "Request too large"
                                    )
                                );

                                req.destroy();
                            }
                        }
                    );


                    req.on(
                        "end",
                        () => {

                            try {

                                resolve(
                                    body
                                        ? JSON.parse(body)
                                        : {}
                                );

                            }
                            catch (error) {

                                reject(error);
                            }
                        }
                    );


                    req.on(
                        "error",
                        reject
                    );
                }
            );
        }

        // =====================================================
        // ELEVENLABS SPEECH-TO-TEXT
        // =====================================================

        async function transcribeWithElevenLabs(
            wavPath
        ) {

            if (!ELEVENLABS_API_KEY) {

                throw new Error(
                    "ELEVENLABS_API_KEY is missing"
                );
            }


            // -------------------------------------------------
            // READ WAV FILE
            // -------------------------------------------------

            const audioBuffer =
                fs.readFileSync(
                    wavPath
                );


            // -------------------------------------------------
            // CREATE NATIVE BLOB
            // -------------------------------------------------

            const audioBlob =
                new Blob(
                    [
                        audioBuffer
                    ],
                    {
                        type: "audio/wav"
                    }
                );


            // -------------------------------------------------
            // CREATE NATIVE MULTIPART FORM
            // -------------------------------------------------

            const formData =
                new FormData();


            formData.append(
                "file",
                audioBlob,
                path.basename(
                    wavPath
                )
            );


            formData.append(
                "model_id",
                "scribe_v2"
            );


            // -------------------------------------------------
            // OPTIONAL LANGUAGE
            // -------------------------------------------------

            formData.append(
                "language_code",
                "eng"
            );


            console.log(
                "Sending WAV to ElevenLabs:"
            );

            console.log(
                "File:",
                path.basename(
                    wavPath
                )
            );

            console.log(
                "Size:",
                audioBuffer.length,
                "bytes"
            );


            // -------------------------------------------------
            // SEND REQUEST
            // -------------------------------------------------

            const response =
                await fetch(
                    "https://api.elevenlabs.io/v1/speech-to-text",
                    {
                        method: "POST",

                        headers: {
                            "xi-api-key":
                                ELEVENLABS_API_KEY
                        },

                        body:
                            formData
                    }
                );


            // -------------------------------------------------
            // READ RESPONSE
            // -------------------------------------------------

            const responseText =
                await response.text();


            if (!response.ok) {

                throw new Error(
                    `ElevenLabs HTTP ${response.status}: ${responseText}`
                );
            }


            // -------------------------------------------------
            // PARSE JSON
            // -------------------------------------------------

            let result;

            try {

                result =
                    JSON.parse(
                        responseText
                    );

            }
            catch (
                error
            ) {

                throw new Error(
                    "ElevenLabs returned invalid JSON: " +
                    responseText
                );
            }


            return result;
        }

        // =====================================================
        // PROCESS ONE TRANSCRIPTION CHUNK
        // =====================================================

        async function processTranscriptionChunk(
            device,
            pcmData,
            chunkNumber
        ) {

            const baseName =
                `${device.recordingId}_chunk_${String(chunkNumber).padStart(4, "0")}`;


            const rawWavPath =
                path.join(
                    tempAudioDir,
                    `${baseName}_${crypto
                        .randomBytes(4)
                        .toString("hex")}_raw.wav`
                );


            const cleanWavPath =
                path.join(
                    tempAudioDir,
                    `${baseName}_${crypto
                        .randomBytes(4)
                        .toString("hex")}_clean.wav`
                );


            try {

                // =================================================
                // 1. CREATE RAW 30-SECOND WAV
                // =================================================

                createWavFromPCM(
                    pcmData,
                    rawWavPath
                );


                console.log(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} WAV created`
                );


                // =================================================
                // 2. PYTHON AUDIO ENHANCEMENT
                // =================================================

                await runAudioProcessor(
                    rawWavPath,
                    cleanWavPath
                );


                console.log(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} audio processing complete`
                );


                // =================================================
                // 3. ELEVENLABS TRANSCRIPTION
                // =================================================

                console.log(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} sending to ElevenLabs...`
                );


                const result =
                    await transcribeWithElevenLabs(
                        cleanWavPath
                    );


                const text =
                    (
                        result.text ||
                        ""
                    ).trim();


                console.log(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} transcript:`,
                    text || "[NO SPEECH]"
                );


                // =================================================
                // 4. STORE TRANSCRIPT IN 500 MB ROLLING CACHE
                // =================================================

                await addTranscriptToCache(
                    device,
                    chunkNumber,
                    text
                );

                // Keep the API response lightweight. The cache can
                // contain up to 500 MB, but we only expose the latest
                // 50 chunks (normally about 25 minutes) as live text.
                const recentEntries =
                    await getDeviceTranscript(
                        device.id
                    );

                const recentTexts =
                    recentEntries
                        .slice(-50)
                        .map(
                            entry =>
                                entry.text
                        )
                        .filter(Boolean);

                device.liveTranscript =
                    recentTexts.join(" ");

                console.log(
                    `[${device.id}] ` +
                    `Transcript cached | ` +
                    `chunk #${chunkNumber}`
                );
            }
            catch (error) {

                console.error(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} transcription failed:`,
                    error.message
                );
            }
            finally {

                // Delete temporary audio immediately.
                for (const filePath of [
                    rawWavPath,
                    cleanWavPath
                ]) {

                    try {

                        if (
                            fs.existsSync(
                                filePath
                            )
                        ) {
                            fs.unlinkSync(
                                filePath
                            );
                        }
                    }
                    catch (cleanupError) {

                        console.error(
                            `[${device.id}] Temporary audio cleanup failed:`,
                            cleanupError.message
                        );
                    }
                }

                console.log(
                    `[${device.id}] ` +
                    `Chunk #${chunkNumber} temporary audio deleted`
                );
            }
        }

        const httpServer =
            http.createServer(
                async (
                    req,
                    res
                ) => {

                    // ---------------------------------------------
                    // CORS PREFLIGHT
                    // ---------------------------------------------

                    if (
                        req.method ===
                        "OPTIONS"
                    ) {

                        res.writeHead(
                            204,
                            {
                                "Access-Control-Allow-Origin":
                                    ALLOWED_ORIGIN,

                                "Access-Control-Allow-Methods":
                                    "GET,POST,OPTIONS",

                                "Access-Control-Allow-Headers":
                                    "Content-Type"
                            }
                        );


                        res.end();

                        return;
                    }


                    const url =
                        new URL(
                            req.url,
                            `http://${req.headers.host}`
                        );


                    const pathname =
                        url.pathname;


                    // =================================================
                    // API: HEALTH
                    // =================================================

                    if (
                        pathname ===
                        "/api/health"
                    ) {

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,

                                server:
                                    "EchoClip",

                                tcpPort:
                                    TCP_PORT,

                                httpPort:
                                    HTTP_PORT,

                                udpPort:
                                    UDP_PORT,

                                udpAuthentication:
                                    UDP_SHARED_TOKEN ? "enabled" : "disabled",

                                udpStats:
                                    udpStats,

                                uptime:
                                    process.uptime(),

                                devices:
                                    devices.size,

                                transcriptCache:
                                    await getTranscriptCacheStats(),

                                time:
                                    new Date().toISOString()
                            }
                        );

                        return;
                    }


                    // =================================================
                    // API: ALL DEVICES
                    // =================================================

                    if (
                        pathname ===
                        "/api/devices" &&
                        req.method === "GET"
                    ) {

                        const list =
                            Array.from(
                                devices.values()
                            )
                                .map(
                                    deviceToJSON
                                );


                        sendJSON(
                            res,
                            200,
                            {
                                success: true,

                                count:
                                    list.length,

                                devices:
                                    list
                            }
                        );

                        return;
                    }


                    // =================================================
                    // API: DEVICE STATUS
                    // =================================================

                    const statusMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/status$/
                        );


                    if (
                        statusMatch &&
                        req.method === "GET"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                statusMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,

                                    error:
                                        "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        sendJSON(
                            res,
                            200,
                            {
                                success: true,

                                device:
                                    deviceToJSON(
                                        device
                                    )
                            }
                        );

                        return;
                    }

                    // =====================================================
                    // API: LIVE TRANSCRIPT
                    // =====================================================

                    const transcriptMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/transcript$/
                        );


                    if (
                        transcriptMatch &&
                        req.method === "GET"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                transcriptMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,
                                    error: "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        const cacheStats =
                            await getTranscriptCacheStats();

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,

                                deviceId:
                                    device.id,

                                recording:
                                    device.recording,

                                transcript:
                                    device.liveTranscript || "",

                                cache:
                                    cacheStats
                            }
                        );

                        return;
                    }

                    // =================================================
                    // API: START
                    // =================================================

                    const startMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/start$/
                        );


                    if (
                        startMatch &&
                        req.method === "POST"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                startMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,

                                    error:
                                        "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        const result =
                            startRecording(
                                device
                            );


                        sendJSON(
                            res,
                            result.success
                                ? 200
                                : 503,
                            result
                        );

                        return;
                    }


                    // =================================================
                    // API: STOP
                    // =================================================

                    const stopMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/stop$/
                        );


                    if (
                        stopMatch &&
                        req.method === "POST"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                stopMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,

                                    error:
                                        "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        const result =
                            stopRecording(
                                device
                            );


                        sendJSON(
                            res,
                            200,
                            result
                        );

                        return;
                    }


                    // =================================================
                    // API: FACTORY RESET
                    // =================================================

                    const resetMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/factory-reset$/
                        );


                    if (
                        resetMatch &&
                        req.method === "POST"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                resetMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,

                                    error:
                                        "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        const result =
                            factoryReset(
                                device
                            );


                        sendJSON(
                            res,
                            result.success
                                ? 200
                                : 503,
                            result
                        );

                        return;
                    }


                    // =================================================
                    // API: PING
                    // =================================================

                    const pingMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/ping$/
                        );


                    if (
                        pingMatch &&
                        req.method === "POST"
                    ) {

                        const deviceId =
                            decodeURIComponent(
                                pingMatch[1]
                            );


                        const device =
                            getDevice(
                                deviceId
                            );


                        if (!device) {

                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,

                                    error:
                                        "DEVICE_NOT_FOUND"
                                }
                            );

                            return;
                        }


                        const sent =
                            sendCommand(
                                device,
                                "PING"
                            );


                        sendJSON(
                            res,
                            sent
                                ? 200
                                : 503,
                            {
                                success:
                                    sent,

                                message:
                                    sent
                                        ? "PING_SENT"
                                        : "DEVICE_OFFLINE"
                            }
                        );

                        return;
                    }


                    // =================================================
                    // API: TRANSCRIPTION CACHE STATUS
                    // =================================================

                    if (
                        pathname ===
                        "/api/transcript-cache" &&
                        req.method === "GET"
                    ) {

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,
                                cache:
                                    await getTranscriptCacheStats()
                            }
                        );

                        return;
                    }


                    // =================================================
                    // API: RECORDINGS
                    // =================================================
                    // Permanent recordings are disabled.

                    if (
                        pathname ===
                        "/api/recordings" &&
                        req.method === "GET"
                    ) {

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,
                                count: 0,
                                recordings: [],
                                message:
                                    "Permanent audio storage is disabled."
                            }
                        );

                        return;
                    }


                    // =================================================
                    // SERVE RECORDINGS
                    // =================================================
                    // No permanent audio files are available.

                    if (
                        pathname.startsWith(
                            "/recordings/"
                        )
                    ) {

                        sendJSON(
                            res,
                            404,
                            {
                                success: false,
                                error:
                                    "PERMANENT_RECORDINGS_DISABLED"
                            }
                        );

                        return;
                    }


                    // =================================================
                    // DASHBOARD
                    // =================================================

                    if (
                        pathname === "/" ||
                        pathname === "/dashboard"
                    ) {

                        sendDashboard(
                            res
                        );

                        return;
                    }


                    // =================================================
                    // 404
                    // =================================================

                    sendJSON(
                        res,
                        404,
                        {
                            success: false,

                            error:
                                "ENDPOINT_NOT_FOUND"
                        }
                    );

                }
            );



        // =====================================================
        // SERVE DASHBOARD.HTML
        // =====================================================

        function sendDashboard(res) {

            const dashboardPath =
                path.join(
                    __dirname,
                    "dashboard.html"
                );

            fs.readFile(
                dashboardPath,
                "utf8",
                (error, html) => {

                    if (error) {

                        console.error(
                            "Dashboard load error:",
                            error.message
                        );

                        res.writeHead(
                            500,
                            {
                                "Content-Type":
                                    "text/plain; charset=utf-8"
                            }
                        );

                        res.end(
                            "Dashboard file not found: " +
                            error.message
                        );

                        return;
                    }

                    res.writeHead(
                        200,
                        {
                            "Content-Type":
                                "text/html; charset=utf-8",

                            "Cache-Control":
                                "no-store"
                        }
                    );

                    res.end(html);
                }
            );
        }

        // =====================================================
        // PROCESS 30-SECOND TRANSCRIPTION CHUNKS
        // =====================================================

        function processTranscriptionBuffer(
            device
        ) {

            if (
                !device ||
                !device.recording
            ) {
                return;
            }


            /*
            * Process every complete 30-second
            * chunk that is currently available.
            */

            while (
                device.transcriptionBufferBytes >=
                TRANSCRIPTION_CHUNK_BYTES
            ) {

                let bytesNeeded =
                    TRANSCRIPTION_CHUNK_BYTES;

                const chunkParts = [];


                while (
                    bytesNeeded > 0 &&
                    device.transcriptionBuffer.length > 0
                ) {

                    const part =
                        device.transcriptionBuffer[0];


                    if (
                        part.length <=
                        bytesNeeded
                    ) {

                        chunkParts.push(
                            part
                        );

                        bytesNeeded -=
                            part.length;

                        device.transcriptionBuffer.shift();

                    }
                    else {

                        const chunkPart =
                            part.subarray(
                                0,
                                bytesNeeded
                            );

                        const remainingPart =
                            part.subarray(
                                bytesNeeded
                            );

                        chunkParts.push(
                            chunkPart
                        );

                        device.transcriptionBuffer[0] =
                            remainingPart;

                        bytesNeeded =
                            0;
                    }
                }


                const chunk =
                    Buffer.concat(
                        chunkParts
                    );


                device.transcriptionBufferBytes -=
                    chunk.length;


                device.transcriptionChunkNumber++;


                const chunkNumber =
                    device.transcriptionChunkNumber;


                console.log(
                    `[${device.id}] ` +
                    `30-sec transcription chunk #${chunkNumber} ready`
                );



                /*
                * Do NOT block the TCP receive handler.
                *
                * The processing happens asynchronously.
                */

                processTranscriptionChunk(
                    device,
                    chunk,
                    chunkNumber
                );
            }
        }


        // =====================================================
        // PROCESS FINAL PARTIAL TRANSCRIPTION CHUNK
        // =====================================================

        async function processRemainingTranscription(
            device
        ) {

            if (
                !device ||
                device.transcriptionBufferBytes <= 0
            ) {
                return;
            }


            const chunkParts = [];

            let bytesNeeded =
                device.transcriptionBufferBytes;


            while (
                bytesNeeded > 0 &&
                device.transcriptionBuffer.length > 0
            ) {

                const part =
                    device.transcriptionBuffer[0];


                if (
                    part.length <= bytesNeeded
                ) {

                    chunkParts.push(
                        part
                    );

                    bytesNeeded -=
                        part.length;

                    device.transcriptionBuffer.shift();

                }
                else {

                    const chunkPart =
                        part.subarray(
                            0,
                            bytesNeeded
                        );

                    chunkParts.push(
                        chunkPart
                    );

                    device.transcriptionBuffer[0] =
                        part.subarray(
                            bytesNeeded
                        );

                    bytesNeeded = 0;
                }
            }


            const chunk =
                Buffer.concat(
                    chunkParts
                );


            device.transcriptionBufferBytes =
                0;


            if (
                chunk.length === 0
            ) {
                return;
            }


            device.transcriptionChunkNumber++;


            const chunkNumber =
                device.transcriptionChunkNumber;


            console.log(
                `[${device.id}] ` +
                `Final transcription chunk #${chunkNumber} ready`
            );


            await processTranscriptionChunk(
                device,
                chunk,
                chunkNumber
            );
        }

        // =====================================================
        // CREATE TEMPORARY WAV FROM PCM BUFFER
        // =====================================================

        function createWavFromPCM(
            pcmData,
            wavPath
        ) {

            const byteRate =
                SAMPLE_RATE *
                CHANNELS *
                BITS_PER_SAMPLE / 8;


            const blockAlign =
                CHANNELS *
                BITS_PER_SAMPLE / 8;


            const header =
                Buffer.alloc(44);


            header.write(
                "RIFF",
                0
            );

            header.writeUInt32LE(
                36 + pcmData.length,
                4
            );

            header.write(
                "WAVE",
                8
            );

            header.write(
                "fmt ",
                12
            );

            header.writeUInt32LE(
                16,
                16
            );

            header.writeUInt16LE(
                1,
                20
            );

            header.writeUInt16LE(
                CHANNELS,
                22
            );

            header.writeUInt32LE(
                SAMPLE_RATE,
                24
            );

            header.writeUInt32LE(
                byteRate,
                28
            );

            header.writeUInt16LE(
                blockAlign,
                32
            );

            header.writeUInt16LE(
                BITS_PER_SAMPLE,
                34
            );

            header.write(
                "data",
                36
            );

            header.writeUInt32LE(
                pcmData.length,
                40
            );


            fs.writeFileSync(
                wavPath,
                Buffer.concat([
                    header,
                    pcmData
                ])
            );
        }

        // Initialize the transcript cache before accepting devices.
        initializeTranscriptCache()
            .catch(error => {
                console.error("Transcript cache initialization error:", error.message);
            });


        // =====================================================
        // START UDP AUDIO SERVER
        // =====================================================

        udpServer.bind(
            UDP_PORT,
            UDP_HOST,
            () => {
                console.log(`UDP audio server running on ${UDP_HOST}:${UDP_PORT}`);
            }
        );


        // =====================================================
        // START TCP SERVER
        // =====================================================

        tcpServer.listen(
            TCP_PORT,
            TCP_HOST,
            () => {

                console.log("");
                console.log(
                    "=========================================="
                );

                console.log(
                    "          ECHOCLIP CLOUD SERVER"
                );

                console.log(
                    "=========================================="
                );

                console.log(
                    `TCP Control : ${TCP_HOST}:${TCP_PORT} `
                );

                console.log(
                    `UDP Audio   : ${UDP_HOST}:${UDP_PORT} `
                );

                console.log(
                    `HTTP API: ${HTTP_HOST}:${HTTP_PORT} `
                );

                console.log(
                    `Sample Rate: ${SAMPLE_RATE} Hz`
                );

                console.log(
                    `Channels: ${CHANNELS} `
                );

                console.log(
                    `Bits: ${BITS_PER_SAMPLE} `
                );

                console.log(
                    "=========================================="
                );

                console.log(
                    "Waiting for EchoClip devices..."
                );
            }
        );


        // =====================================================
        // START HTTP SERVER
        // =====================================================

        httpServer.listen(
            HTTP_PORT,
            HTTP_HOST,
            () => {

                console.log(
                    `HTTP server running on port ${HTTP_PORT} `
                );

                console.log(
                    `Dashboard: http://localhost:${HTTP_PORT}`
                );

                console.log("");
            }
        );

        // =====================================================
        // GRACEFUL SHUTDOWN
        // =====================================================

        let shuttingDown = false;

        async function shutdown(signal) {
            if (shuttingDown) return;
            shuttingDown = true;

            console.log(`Received ${signal}; shutting down EchoClip cleanly...`);
            clearInterval(udpFlushTimer);

            for (const device of devices.values()) {
                try {
                    if (device.recording) {
                        device.recording = false;
                        await processRemainingTranscription(device);
                    }
                } catch (error) {
                    console.error(`[${device.id}] shutdown transcription failed:`, error.message);
                }

                try {
                    device.socket?.destroy();
                } catch (_) {}
            }

            await new Promise(resolve => udpServer.close(() => resolve()));
            await new Promise(resolve => tcpServer.close(() => resolve()));
            await new Promise(resolve => httpServer.close(() => resolve()));

            try {
                if (redisClient && redisReady) {
                    await redisClient.quit();
                }
            } catch (_) {}

            process.exit(0);
        }

        process.on("SIGTERM", () => shutdown("SIGTERM"));
        process.on("SIGINT", () => shutdown("SIGINT"));
