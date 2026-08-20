require("dotenv").config();
const net = require("net");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const {
    execFile
} = require("child_process");
// =====================================================
// CONFIGURATION
// =====================================================

const TCP_HOST = "0.0.0.0";
const TCP_PORT = 5000;

const HTTP_HOST = "0.0.0.0";
const HTTP_PORT = 3000;

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

if (!ELEVENLABS_API_KEY) {

    console.error(
        "ERROR: ELEVENLABS_API_KEY is not configured."
    );

}

// =====================================================
// DIRECTORIES
// =====================================================

const recordingsDir =
    path.join(__dirname, "recordings");

if (!fs.existsSync(recordingsDir)) {
    fs.mkdirSync(
        recordingsDir,
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

function createWavFile(
    pcmPath,
    wavPath
) {

    const pcmData =
        fs.readFileSync(pcmPath);


    const byteRate =
        SAMPLE_RATE *
        CHANNELS *
        BITS_PER_SAMPLE / 8;


    const blockAlign =
        CHANNELS *
        BITS_PER_SAMPLE / 8;


    const header =
        Buffer.alloc(44);


    // RIFF

    header.write(
        "RIFF",
        0
    );


    header.writeUInt32LE(
        36 + pcmData.length,
        4
    );


    // WAVE

    header.write(
        "WAVE",
        8
    );


    // fmt

    header.write(
        "fmt ",
        12
    );


    header.writeUInt32LE(
        16,
        16
    );


    // PCM

    header.writeUInt16LE(
        1,
        20
    );


    // channels

    header.writeUInt16LE(
        CHANNELS,
        22
    );


    // sample rate

    header.writeUInt32LE(
        SAMPLE_RATE,
        24
    );


    // byte rate

    header.writeUInt32LE(
        byteRate,
        28
    );


    // block align

    header.writeUInt16LE(
        blockAlign,
        32
    );


    // bits

    header.writeUInt16LE(
        BITS_PER_SAMPLE,
        34
    );


    // data

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

        pcmFile:
            null,

        pcmPath:
            null,

        wavPath:
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
        transcriptionResults: {},

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
    // FILE NAMES
    // -------------------------------------------------

    const timestamp =
        getTimestamp();


    const recordingId =
        `${device.id}_${timestamp}`;


    const pcmPath =
        path.join(
            recordingsDir,
            `${recordingId}.pcm`
        );


    const wavPath =
        path.join(
            recordingsDir,
            `${recordingId}.wav`
        );


    device.recordingId =
        recordingId;


    device.pcmPath =
        pcmPath;


    device.wavPath =
        wavPath;


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
    device.transcriptionResults = {};

    device.liveTranscript =
        "";
    device.expectingRecordingData =
        false;

    device.recordingStarted =
        Date.now();

    device.recording =
        true;

    device.pcmFile =
        fs.createWriteStream(
            pcmPath
        );


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
        device.pcmFile.end();

        device.pcmFile =
            null;

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

    if (!device.pcmFile) {

        device.recording =
            false;

        return;
    }


    const pcmFile =
        device.pcmFile;


    const pcmPath =
        device.pcmPath;


    const wavPath =
        device.wavPath;


    device.pcmFile =
        null;


    pcmFile.end(
        () => {

            try {

                createWavFile(
                    pcmPath,
                    wavPath
                );


                console.log(
                    `[${device.id}] WAV created: ${wavPath}`
                );


                const duration =
                    device.recordingBytes /
                    (
                        SAMPLE_RATE *
                        CHANNELS *
                        (BITS_PER_SAMPLE / 8)
                    );


                console.log(
                    `[${device.id}] Duration: ${duration.toFixed(2)} sec`
                );


            }
            catch (error) {

                console.error(
                    "WAV creation error:",
                    error.message
                );
            }
        }
    );
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
                    !device.recording ||
                    !device.pcmFile
                ) {
                    return;
                }


                if (
                    audioData.length === 0
                ) {
                    return;
                }


                device.pcmFile.write(
                    audioData
                );


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
            recordingsDir,
            `${baseName}_raw.wav`
        );


    const cleanWavPath =
        path.join(
            recordingsDir,
            `${baseName}_clean.wav`
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
        // 4. STORE TRANSCRIPT IN CHUNK ORDER
        // =================================================

        device.transcriptionResults[
            chunkNumber
        ] = text;


        // Rebuild transcript in correct order

        const orderedTexts =
            Object.keys(
                device.transcriptionResults
            )
                .sort(
                    (a, b) =>
                        Number(a) - Number(b)
                )
                .map(
                    key =>
                        device.transcriptionResults[key]
                )
                .filter(
                    text =>
                        text &&
                        text.length > 0
                );


        device.liveTranscript =
            orderedTexts.join(" ");


        console.log(
            `[${device.id}] ` +
            `Live transcript updated`
        );


    }
    catch (error) {

        console.error(
            `[${device.id}] ` +
            `Chunk #${chunkNumber} transcription failed:`,
            error.message
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

                        uptime:
                            process.uptime(),

                        devices:
                            devices.size,

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

                        deviceId:
                            device.id,

                        recording:
                            device.recording,

                        transcript:
                            device.liveTranscript || ""
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
            // API: RECORDINGS
            // =================================================

            if (
                pathname ===
                "/api/recordings" &&
                req.method === "GET"
            ) {

                const files =
                    fs.readdirSync(
                        recordingsDir
                    );


                const recordings =
                    files
                        .filter(
                            file =>
                                file.endsWith(
                                    ".wav"
                                )
                        )
                        .map(
                            file => {

                                const fullPath =
                                    path.join(
                                        recordingsDir,
                                        file
                                    );


                                const stat =
                                    fs.statSync(
                                        fullPath
                                    );


                                return {

                                    file,

                                    size:
                                        stat.size,

                                    created:
                                        stat.birthtime
                                            .toISOString(),

                                    url:
                                        `/recordings/${encodeURIComponent(file)}`
                                };
                            }
                        )
                        .sort(
                            (
                                a,
                                b
                            ) =>
                                b.created.localeCompare(
                                    a.created
                                )
                        );


                sendJSON(
                    res,
                    200,
                    {
                        success: true,

                        count:
                            recordings.length,

                        recordings
                    }
                );

                return;
            }


            // =================================================
            // SERVE RECORDINGS
            // =================================================

            if (
                pathname.startsWith(
                    "/recordings/"
                )
            ) {

                const filename =
                    decodeURIComponent(
                        pathname.substring(
                            "/recordings/"
                                .length
                        )
                    );


                // Prevent path traversal

                const safeName =
                    path.basename(
                        filename
                    );


                const filePath =
                    path.join(
                        recordingsDir,
                        safeName
                    );


                if (
                    !fs.existsSync(
                        filePath
                    )
                ) {

                    res.writeHead(
                        404
                    );

                    res.end(
                        "Not found"
                    );

                    return;
                }


                res.writeHead(
                    200,
                    {
                        "Content-Type":
                            "audio/wav",

                        "Access-Control-Allow-Origin":
                            "*"
                    }
                );


                fs.createReadStream(
                    filePath
                )
                    .pipe(
                        res
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
            `TCP Audio / Control : ${TCP_HOST}:${TCP_PORT} `
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