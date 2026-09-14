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

        // Permanent recordings are stored as WAV files on disk.
        // Temporary WAV files are used only for transcription.
        const os = require("os");

        const recordingsDir =
            path.join(
                __dirname,
                "recordings"
            );

        // One plain-text transcript file per recording session.
        const transcriptsDir =
            path.join(
                __dirname,
                "transcripts"
            );

        const tempAudioDir =
            path.join(
                os.tmpdir(),
                "echoclip-transcription"
            );

        for (const dir of [recordingsDir, transcriptsDir, tempAudioDir]) {
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(
                    dir,
                    { recursive: true }
                );
            }
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

                recordingFilePath:
                    null,

                recordingFd:
                    null,

                recordingStorageError:
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

                // Transcript state belongs only to the current recording session.
                liveTranscriptTexts:
                    [],

                transcriptionQueue:
                    Promise.resolve(),

                transcriptFilePath:
                    null,

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
        // PHYSICAL BUTTON EVENT FROM DEVICE
        // =====================================================
        //
        // ESP32 sends BUTTON_START / BUTTON_STOP.
        // The server owns the recording state and responds with
        // the normal START / STOP command.
        //
        async function handlePhysicalButton(device, event)
        {
            if (!device || !device.connected)
                return;

            const normalized =
                String(event || "").trim().toUpperCase();

            if (normalized === "BUTTON_START")
            {
                console.log(
                    `[${device.id}] PHYSICAL BUTTON -> START`
                );

                if (device.recording)
                {
                    sendCommand(device, "BUTTON_ACK RECORDING");
                    return;
                }

                const result = startRecording(device);

                if (result.success)
                {
                    sendCommand(device, "BUTTON_ACK STARTED");
                    console.log(
                        `[${device.id}] Physical button START accepted`
                    );
                }
                else
                {
                    sendCommand(device, "BUTTON_ACK ERROR");
                    console.error(
                        `[${device.id}] Physical button START failed:`,
                        result.error
                    );
                }

                return;
            }

            if (normalized === "BUTTON_STOP")
            {
                console.log(
                    `[${device.id}] PHYSICAL BUTTON -> STOP`
                );

                if (!device.recording)
                {
                    sendCommand(device, "BUTTON_ACK NOT_RECORDING");
                    return;
                }

                const result = await stopRecording(device);

                if (result.success)
                {
                    sendCommand(device, "BUTTON_ACK STOPPED");
                    console.log(
                        `[${device.id}] Physical button STOP accepted`
                    );
                }
                else
                {
                    sendCommand(device, "BUTTON_ACK ERROR");
                    console.error(
                        `[${device.id}] Physical button STOP failed:`,
                        result.error
                    );
                }

                return;
            }

            console.log(
                `[${device.id}] Unknown device event: ${normalized}`
            );
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
            // A permanent WAV file is opened immediately.
            // Incoming PCM is appended directly to disk so
            // long recordings do not consume server RAM.

            const timestamp =
                getTimestamp();

            const recordingId =
                `${device.id}_${timestamp}`;

            const recordingFilePath =
                path.join(
                    recordingsDir,
                    `${recordingId}.wav`
                );

            try {

                if (fs.existsSync(recordingFilePath)) {
                    throw new Error(
                        "Recording file already exists"
                    );
                }

                const fd =
                    fs.openSync(
                        recordingFilePath,
                        "w"
                    );

                // Reserve the standard 44-byte WAV header.
                fs.writeSync(
                    fd,
                    createWavHeader(0),
                    0,
                    44,
                    0
                );

                device.recordingFd = fd;
                device.recordingFilePath = recordingFilePath;
                device.recordingStorageError = null;
            }
            catch (error) {

                console.error(
                    `[${device.id}] Unable to create recording file:`,
                    error.message
                );

                return {
                    success: false,
                    error: "RECORDING_STORAGE_FAILED",
                    details: error.message
                };
            }

            device.recordingId =
                recordingId;

            device.transcriptFilePath =
                path.join(
                    transcriptsDir,
                    `${recordingId}.txt`
                );

            try {
                // Keep transcript files plain and frontend-friendly:
                // timestamp + transcript text only. No session-finalized marker.
                fs.writeFileSync(
                    device.transcriptFilePath,
                    "",
                    "utf8"
                );
            }
            catch (error) {
                closeRecordingFile(device, true);
                device.transcriptFilePath = null;
                return {
                    success: false,
                    error: "TRANSCRIPT_STORAGE_FAILED",
                    details: error.message
                };
            }

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

            // Reset live transcript completely for the new recording session.
            device.liveTranscriptTexts = [];
            device.transcriptionQueue = Promise.resolve();
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
                closeRecordingFile(device, true);
                if (device.transcriptFilePath && fs.existsSync(device.transcriptFilePath)) {
                    try { fs.unlinkSync(device.transcriptFilePath); } catch (_) {}
                }
                device.transcriptFilePath = null;

                return {
                    success: false,
                    error: "COMMAND_FAILED"
                };
            }


            console.log(
                `[${device.id}] Recording started | ` +
                `WAV: ${recordingFilePath}`
            );


            return {
                success: true,
                recordingId: recordingId,
                recordingFile: path.basename(recordingFilePath),
                message: "RECORDING_STARTED"
            };
        }


        // =====================================================
        // STOP RECORDING
        // =====================================================

        async function stopRecording(
            device
        ) {

            if (!device) {

                return {
                    success: false,
                    error: "DEVICE_NOT_FOUND"
                };
            }


            if (!device.recording) {

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

            device.recording = false;


            // -------------------------------------------------
            // FINAL TRANSCRIPTION CHUNK
            // -------------------------------------------------

            try {
                await processRemainingTranscription(device);
                await device.transcriptionQueue;
            }
            catch (error) {
                console.error(
                    `[${device.id}] Final transcription failed:`,
                    error.message
                );
            }

            // -------------------------------------------------
            // FINALIZE PERMANENT WAV
            // -------------------------------------------------

            const saved =
                finishRecording(device);


            return {
                success: true,
                recordingId: device.recordingId,
                recordingFile: saved.file,
                recordingBytes: device.recordingBytes,
                recordingDuration: Number(
                    (
                        device.recordingBytes /
                        (SAMPLE_RATE * CHANNELS * (BITS_PER_SAMPLE / 8))
                    ).toFixed(2)
                ),
                recordingUrl: saved.url,
                message: saved.error
                    ? "RECORDING_STOPPED_STORAGE_ERROR"
                    : "RECORDING_STOPPED"
            };
        }


        // =====================================================
        // CLOSE / FINALIZE PERMANENT RECORDING
        // =====================================================

        function closeRecordingFile(
            device,
            deleteFile = false
        ) {

            if (!device) {
                return;
            }

            const fd =
                device.recordingFd;

            const filePath =
                device.recordingFilePath;

            try {

                if (fd !== null && fd !== undefined) {
                    fs.closeSync(fd);
                }
            }
            catch (error) {
                console.error(
                    `[${device.id}] Recording file close failed:`,
                    error.message
                );
            }

            device.recordingFd = null;

            if (
                deleteFile &&
                filePath &&
                fs.existsSync(filePath)
            ) {
                try {
                    fs.unlinkSync(filePath);
                }
                catch (error) {
                    console.error(
                        `[${device.id}] Failed to delete incomplete recording:`,
                        error.message
                    );
                }
            }
        }


        function finishRecording(
            device
        ) {

            if (!device) {
                return {
                    file: null,
                    url: null,
                    error: "DEVICE_NOT_FOUND"
                };
            }

            const filePath =
                device.recordingFilePath;

            if (!filePath || device.recordingFd === null) {
                device.recording = false;

                return {
                    file: null,
                    url: null,
                    error: "RECORDING_FILE_NOT_OPEN"
                };
            }

            try {

                // Patch the WAV header with the final PCM size.
                const header =
                    createWavHeader(
                        device.recordingBytes
                    );

                fs.writeSync(
                    device.recordingFd,
                    header,
                    0,
                    header.length,
                    0
                );

                fs.closeSync(
                    device.recordingFd
                );

                device.recordingFd = null;
                device.recording = false;

                const stat =
                    fs.statSync(filePath);

                const expectedFileSize =
                    44 + device.recordingBytes;

                const storageVerified =
                    stat.size === expectedFileSize;

                console.log(
                    `[${device.id}] Recording saved | ` +
                    `${path.basename(filePath)} | ` +
                    `${stat.size} bytes | ` +
                    `${(
                        device.recordingBytes /
                        (SAMPLE_RATE * CHANNELS * (BITS_PER_SAMPLE / 8))
                    ).toFixed(2)} sec | ` +
                    `storage ${storageVerified ? "OK" : "MISMATCH"}`
                );

                if (!storageVerified) {
                    console.error(
                        `[${device.id}] WAV size mismatch: ` +
                        `expected ${expectedFileSize} bytes, ` +
                        `got ${stat.size} bytes`
                    );
                }

                return {
                    file: path.basename(filePath),
                    url: `/recordings/${encodeURIComponent(path.basename(filePath))}`,
                    size: stat.size
                };
            }
            catch (error) {

                device.recordingStorageError =
                    error.message;

                console.error(
                    `[${device.id}] Recording finalization failed:`,
                    error.message
                );

                closeRecordingFile(device, false);
                device.recording = false;

                return {
                    file: filePath
                        ? path.basename(filePath)
                        : null,
                    url: filePath
                        ? `/recordings/${encodeURIComponent(path.basename(filePath))}`
                        : null,
                    error: error.message
                };
            }
        }


        // =====================================================
        // FACTORY RESET
        // =====================================================


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
                    ),

                recordingFile:
                    device.recordingFilePath
                        ? path.basename(device.recordingFilePath)
                        : null,

                recordingUrl:
                    device.recordingFilePath
                        ? `/recordings/${encodeURIComponent(path.basename(device.recordingFilePath))}`
                        : null,

                recordingStorageError:
                    device.recordingStorageError || null
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

                    // TCP control messages from the ESP32 are newline-delimited.
                    // TCP may split or combine messages, so buffer them.
                    let deviceCommandBuffer = "";






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
                            // TCP CONTROL EVENTS FROM DEVICE
                            // =================================================
                            //
                            // UDP carries the actual PCM audio.
                            // TCP carries control/status messages.
                            //
                            deviceCommandBuffer += data.toString("utf8");

                            if (deviceCommandBuffer.length > 4096)
                            {
                                deviceCommandBuffer =
                                    deviceCommandBuffer.slice(-4096);
                            }

                            let newlineIndex;

                            while (
                                (newlineIndex =
                                    deviceCommandBuffer.indexOf("\n")) !== -1
                            )
                            {
                                const line =
                                    deviceCommandBuffer
                                        .slice(0, newlineIndex)
                                        .replace(/\r/g, "")
                                        .trim();

                                deviceCommandBuffer =
                                    deviceCommandBuffer.slice(
                                        newlineIndex + 1
                                    );

                                if (!line)
                                    continue;

                                // Physical button events.
                                if (
                                    line === "BUTTON_START" ||
                                    line === "BUTTON_STOP"
                                )
                                {
                                    handlePhysicalButton(
                                        device,
                                        line
                                    );

                                    continue;
                                }

                                // UDP-mode marker.
                                if (line === "RECORDING_UDP")
                                {
                                    device.expectingRecordingData = false;

                                    console.log(
                                        `[${device.id}] UDP audio mode armed`
                                    );

                                    continue;
                                }

                                // Heartbeat response.
                                if (line === "PONG")
                                {
                                    device.lastSeen = Date.now();

                                    console.log(
                                        `[${device.id}] PONG`
                                    );

                                    continue;
                                }

                                // Legacy recording marker.
                                if (line === "RECORDING")
                                {
                                    device.expectingRecordingData = true;

                                    console.log(
                                        `[${device.id}] Audio stream started`
                                    );

                                    continue;
                                }

                                console.log(
                                    `[${device.id}] TCP message: ${line}`
                                );
                            }

                            // Current protocol sends audio through UDP :5001.
                            // Do not interpret TCP data as PCM.

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


                        // =================================================
                        // PERMANENT RECORDING
                        // =================================================
                        // IMPORTANT:
                        // The ESP32 in the current setup sends RAW PCM over
                        // TCP. The previous version only counted the bytes
                        // and placed them in the transcription buffer.
                        // It never wrote TCP audio to the permanent WAV.
                        //
                        // That is why the final WAV was only 44 bytes:
                        // it contained the reserved WAV header but no PCM.
                        //
                        // Write the EXACT incoming PCM bytes to the WAV file
                        // before sending a copy to the transcription buffer.
                        // =================================================

                        try {

                            if (
                                device.recordingFd !== null &&
                                device.recordingFd !== undefined
                            ) {

                                fs.writeSync(
                                    device.recordingFd,
                                    audioData,
                                    0,
                                    audioData.length
                                );

                            }
                            else {

                                device.recordingStorageError =
                                    "Recording file descriptor is not open";

                                console.error(
                                    `[${device.id}] Permanent recording write skipped: ` +
                                    `recording file descriptor is not open`
                                );

                                return;
                            }

                        }
                        catch (error) {

                            device.recordingStorageError =
                                error.message;

                            console.error(
                                `[${device.id}] Permanent recording write failed:`,
                                error.message
                            );

                            closeRecordingFile(
                                device,
                                false
                            );

                            return;
                        }


                        // =================================================
                        // RECORDING BYTE COUNTERS
                        // =================================================

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
                            .then(() => device.transcriptionQueue)
                            .catch(
                                error => {
                                    console.error(
                                        `[${device.id}] Final transcription on disconnect failed:`,
                                        error.message
                                    );
                                }
                            )
                            .finally(() => {
                                finishRecording(device);
                            });
                        }


                        console.log(
                            `[${device.id}] DISCONNECTED`
                        );
                    }

                }
            );


        // =====================================================
        // UDP AUDIO SERVER (ESP32 -> AWS)
        // =====================================================

        const udpServer = dgram.createSocket("udp4");

        const udpStats = {
            packets: 0,
            audioPackets: 0,
            audioBytes: 0,
            malformed: 0,
            unknownDevice: 0,
            droppedSequence: 0
        };

        const udpLastSequence = new Map();

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
            if (headerSize + payloadBytes > message.length) return null;

            const deviceId = message
                .subarray(22, 22 + UDP_DEVICE_ID_LEN)
                .toString("utf8")
                .replace(/\0.*$/, "")
                .trim();

            if (!deviceId) return null;

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

            // Persist the exact incoming PCM stream to the permanent WAV.
            try {
                if (device.recordingFd !== null && device.recordingFd !== undefined) {
                    fs.writeSync(
                        device.recordingFd,
                        payload,
                        0,
                        payload.length
                    );
                }
            }
            catch (error) {
                device.recordingStorageError = error.message;
                console.error(
                    `[${device.id}] Permanent recording write failed:`,
                    error.message
                );
                closeRecordingFile(device, false);
            }

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
                udpLastSequence.set(packet.deviceId, packet.sequence);
                device.expectingRecordingData = true;
                console.log(`[${device.id}] UDP AUDIO START from ${rinfo.address}:${rinfo.port}`);
                return;
            }

            if (packet.packetType === UDP_PACKET_END) {
                udpLastSequence.delete(packet.deviceId);
                console.log(`[${device.id}] UDP AUDIO END`);
                return;
            }

            if (packet.packetType !== UDP_PACKET_AUDIO) return;
            if (!device.recording) return;

            // Validate the format expected by the server/transcription pipeline.
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

            const previous = udpLastSequence.get(packet.deviceId);
            if (previous !== undefined) {
                const expected = (previous + 1) >>> 0;
                if (packet.sequence !== expected) {
                    const gap = (packet.sequence - expected) >>> 0;
                    if (gap < 1000000) udpStats.droppedSequence += gap;
                }
            }
            udpLastSequence.set(packet.deviceId, packet.sequence);

            udpStats.audioPackets++;
            udpStats.audioBytes += packet.payload.length;
            ingestUdpAudio(device, packet.payload);
        });

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
                        "*",

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
        // APPEND CURRENT SESSION TRANSCRIPT TO FILE
        // =====================================================

        function appendTranscriptToFile(device, text) {
            const normalizedText = String(text || "").trim();

            if (!device || !device.transcriptFilePath || !normalizedText) {
                return;
            }

            fs.appendFileSync(
                device.transcriptFilePath,
                `[${new Date().toISOString()}]\n${normalizedText}\n\n`,
                "utf8"
            );
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
                // 4. STORE TRANSCRIPT IN CACHE + CURRENT SESSION FILE
                // =================================================

                await addTranscriptToCache(
                    device,
                    chunkNumber,
                    text
                );

                if (text) {
                    // This array is reset at every recording start, so
                    // previous sessions can never leak into live transcript.
                    device.liveTranscriptTexts.push(text);
                    device.liveTranscript =
                        device.liveTranscriptTexts.join(" ");

                    appendTranscriptToFile(
                        device,
                        text
                    );
                }

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
                                    "*",

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


                        sendJSON(
                            res,
                            200,
                            {
                                success: true,
                                deviceId: device.id,
                                recording: device.recording,
                                transcript: device.liveTranscript || ""
                            }
                        );

                        return;
                    }

                    // =================================================
                    // API: TRANSCRIPT HISTORY
                    // =================================================
                    // Returns only transcript files and their timestamps.
                    // Transcript contents are stored in /transcripts.

                    const transcriptsMatch =
                        pathname.match(
                            /^\/api\/devices\/([^/]+)\/transcripts$/
                        );

                    if (
                        transcriptsMatch &&
                        req.method === "GET"
                    ) {
                        const deviceId =
                            decodeURIComponent(
                                transcriptsMatch[1]
                            );

                        let transcripts = [];

                        try {
                            transcripts =
                                fs.readdirSync(transcriptsDir)
                                    .filter(file =>
                                        file.startsWith(`${deviceId}_`) &&
                                        file.toLowerCase().endsWith(".txt")
                                    )
                                    .map(file => {
                                        const fullPath =
                                            path.join(transcriptsDir, file);
                                        const stat = fs.statSync(fullPath);

                                        const timestampMatch =
                                            file.match(/_(\d{8}_\d{6})\.txt$/);

                                        let timestamp = stat.birthtime.toISOString();
                                        if (timestampMatch) {
                                            const raw = timestampMatch[1];
                                            const y = raw.slice(0, 4);
                                            const mo = raw.slice(4, 6);
                                            const d = raw.slice(6, 8);
                                            const h = raw.slice(9, 11);
                                            const mi = raw.slice(11, 13);
                                            const se = raw.slice(13, 15);
                                            timestamp = `${y}-${mo}-${d}T${h}:${mi}:${se}`;
                                        }

                                        return {
                                            file,
                                            timestamp
                                        };
                                    })
                                    .sort((a, b) =>
                                        b.timestamp.localeCompare(a.timestamp)
                                    );
                        }
                        catch (error) {
                            sendJSON(
                                res,
                                500,
                                {
                                    success: false,
                                    error: "TRANSCRIPTS_LIST_FAILED",
                                    details: error.message
                                }
                            );
                            return;
                        }

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,
                                transcripts
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
                            await stopRecording(
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

                    if (
                        pathname ===
                        "/api/recordings" &&
                        req.method === "GET"
                    ) {

                        let recordings = [];

                        try {
                            recordings =
                                fs.readdirSync(
                                    recordingsDir
                                )
                                .filter(
                                    file =>
                                        file.toLowerCase().endsWith(".wav")
                                )
                                .map(
                                    file => {
                                        const fullPath =
                                            path.join(
                                                recordingsDir,
                                                file
                                            );

                                        const stat =
                                            fs.statSync(fullPath);

                                        const pcmBytes =
                                            Math.max(
                                                0,
                                                stat.size - 44
                                            );

                                        return {
                                            file: file,
                                            size: stat.size,
                                            duration: Number(
                                                (
                                                    pcmBytes /
                                                    (SAMPLE_RATE * CHANNELS * (BITS_PER_SAMPLE / 8))
                                                ).toFixed(2)
                                            ),
                                            created: stat.birthtime.toISOString(),
                                            modified: stat.mtime.toISOString(),
                                            url: `/recordings/${encodeURIComponent(file)}`
                                        };
                                    }
                                )
                                .sort(
                                    (a, b) =>
                                        b.created.localeCompare(a.created)
                                );
                        }
                        catch (error) {
                            sendJSON(
                                res,
                                500,
                                {
                                    success: false,
                                    error: "RECORDINGS_LIST_FAILED",
                                    details: error.message
                                }
                            );
                            return;
                        }

                        sendJSON(
                            res,
                            200,
                            {
                                success: true,
                                count: recordings.length,
                                recordings: recordings
                            }
                        );

                        return;
                    }


                    // =================================================
                    // SERVE RECORDINGS WITH HTTP RANGE SUPPORT
                    // =====================================================

                    if (
                        pathname.startsWith(
                            "/recordings/"
                        ) &&
                        req.method === "GET"
                    ) {

                        const requestedName =
                            decodeURIComponent(
                                pathname.substring(
                                    "/recordings/".length
                                )
                            );

                        const safeName =
                            path.basename(requestedName);

                        if (safeName !== requestedName || !safeName.toLowerCase().endsWith(".wav")) {
                            sendJSON(
                                res,
                                400,
                                {
                                    success: false,
                                    error: "INVALID_RECORDING_NAME"
                                }
                            );
                            return;
                        }

                        const filePath =
                            path.join(
                                recordingsDir,
                                safeName
                            );

                        if (!fs.existsSync(filePath)) {
                            sendJSON(
                                res,
                                404,
                                {
                                    success: false,
                                    error: "RECORDING_NOT_FOUND"
                                }
                            );
                            return;
                        }

                        const stat =
                            fs.statSync(filePath);

                        const range =
                            req.headers.range;

                        if (!range) {
                            res.writeHead(
                                200,
                                {
                                    "Content-Type": "audio/wav",
                                    "Content-Length": stat.size,
                                    "Accept-Ranges": "bytes",
                                    "Cache-Control": "no-cache"
                                }
                            );

                            fs.createReadStream(filePath).pipe(res);
                            return;
                        }

                        const match =
                            range.match(/bytes=(\d*)-(\d*)/);

                        if (!match) {
                            res.writeHead(
                                416,
                                {
                                    "Content-Range": `bytes */${stat.size}`
                                }
                            );
                            res.end();
                            return;
                        }

                        let startByte =
                            match[1] ? Number(match[1]) : Math.max(0, stat.size - Number(match[2]));

                        let endByte =
                            match[2] ? Number(match[2]) : stat.size - 1;

                        if (Number.isNaN(startByte) || Number.isNaN(endByte) || startByte > endByte || startByte >= stat.size) {
                            res.writeHead(
                                416,
                                {
                                    "Content-Range": `bytes */${stat.size}`
                                }
                            );
                            res.end();
                            return;
                        }

                        endByte =
                            Math.min(
                                endByte,
                                stat.size - 1
                            );

                        const chunkSize =
                            endByte - startByte + 1;

                        res.writeHead(
                            206,
                            {
                                "Content-Type": "audio/wav",
                                "Content-Length": chunkSize,
                                "Content-Range": `bytes ${startByte}-${endByte}/${stat.size}`,
                                "Accept-Ranges": "bytes",
                                "Cache-Control": "no-cache"
                            }
                        );

                        fs.createReadStream(
                            filePath,
                            {
                                start: startByte,
                                end: endByte
                            }
                        ).pipe(res);

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

                device.transcriptionQueue =
                    device.transcriptionQueue
                        .then(() =>
                            processTranscriptionChunk(
                                device,
                                chunk,
                                chunkNumber
                            )
                        )
                        .catch(error => {
                            console.error(
                                `[${device.id}] Queued transcription failed:`,
                                error.message
                            );
                        });
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


            device.transcriptionQueue =
                device.transcriptionQueue
                    .then(() =>
                        processTranscriptionChunk(
                            device,
                            chunk,
                            chunkNumber
                        )
                    );

            await device.transcriptionQueue;
        }

        // =====================================================
        // WAV HELPERS
        // =====================================================

        function createWavHeader(
            pcmBytes
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

            header.write("RIFF", 0);
            header.writeUInt32LE(36 + pcmBytes, 4);
            header.write("WAVE", 8);
            header.write("fmt ", 12);
            header.writeUInt32LE(16, 16);
            header.writeUInt16LE(1, 20);
            header.writeUInt16LE(CHANNELS, 22);
            header.writeUInt32LE(SAMPLE_RATE, 24);
            header.writeUInt32LE(byteRate, 28);
            header.writeUInt16LE(blockAlign, 32);
            header.writeUInt16LE(BITS_PER_SAMPLE, 34);
            header.write("data", 36);
            header.writeUInt32LE(pcmBytes, 40);

            return header;
        }


        function createWavFromPCM(
            pcmData,
            wavPath
        ) {

            const header =
                createWavHeader(
                    pcmData.length
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

                console.log(
                    "Physical button protocol: BUTTON_START / BUTTON_STOP"
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