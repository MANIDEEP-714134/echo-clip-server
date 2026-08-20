import numpy as np
import soundfile as sf


# =====================================================
# VERSION B - LOW FREQUENCY CLEANUP
# =====================================================
#
# Pipeline:
#
# recording_clean.wav
#       ↓
# DC removal
#       ↓
# Proper 2nd-order Butterworth high-pass @ 90 Hz
#       ↓
# Conservative fixed spectral denoise
#       ↓
# Very gentle compression
#       ↓
# 8x gain
#       ↓
# Limiter
#       ↓
# recording_final.wav
#
# IMPORTANT:
# - No voice gate
# - No adaptive noise profile
# - No notch filters around the speech region
# - No scipy required
#
# This version is intended to test whether the low-frequency
# "turr"/rumble is the main source of the remaining noise.


# =====================================================
# FILE SETTINGS
# =====================================================

import sys

INPUT_FILE = sys.argv[1]
OUTPUT_FILE = sys.argv[2]

# =====================================================
# GAIN
# =====================================================

FINAL_GAIN = 8.0


# =====================================================
# HIGH-PASS
# =====================================================

HIGH_PASS_CUTOFF = 90.0


# =====================================================
# FIXED SPECTRAL DENOISE
# =====================================================

NOISE_PROFILE_SECONDS = 0.5

OVERSUBTRACTION = 1.08
NOISE_FLOOR = 0.20


# =====================================================
# LIGHT COMPRESSION
# =====================================================

COMPRESSION_THRESHOLD = 0.10
COMPRESSION_RATIO = 2.0


# =====================================================
# LOAD
# =====================================================

audio, sample_rate = sf.read(
    INPUT_FILE,
    dtype="float32"
)

if audio.ndim > 1:
    audio = np.mean(audio, axis=1)

audio = audio.astype(np.float64)


# =====================================================
# ORIGINAL INFORMATION
# =====================================================

original_peak = np.max(np.abs(audio))
original_rms = np.sqrt(np.mean(audio ** 2))
duration = len(audio) / sample_rate



# =====================================================
# 1. REMOVE DC OFFSET
# =====================================================

audio -= np.mean(audio)


# =====================================================
# 2. PROPER 2ND-ORDER BUTTERWORTH HIGH-PASS
# =====================================================
#
# RBJ biquad implementation.
#
# This avoids scipy completely.
#
# Q for a Butterworth 2nd-order section:
#
# Q = 1 / sqrt(2)
#
# The filter is applied in two passes (forward/backward)
# to avoid introducing an audible phase shift.


def butterworth_highpass(
    signal,
    fs,
    cutoff
):
    w0 = (
        2.0
        * np.pi
        * cutoff
        / fs
    )

    cos_w0 = np.cos(w0)
    sin_w0 = np.sin(w0)

    Q = 1.0 / np.sqrt(2.0)

    alpha = (
        sin_w0
        /
        (2.0 * Q)
    )

    b0 = (
        (1.0 + cos_w0)
        / 2.0
    )

    b1 = (
        -(1.0 + cos_w0)
    )

    b2 = (
        (1.0 + cos_w0)
        / 2.0
    )

    a0 = (
        1.0
        + alpha
    )

    a1 = (
        -2.0 * cos_w0
    )

    a2 = (
        1.0
        - alpha
    )

    b0 /= a0
    b1 /= a0
    b2 /= a0
    a1 /= a0
    a2 /= a0

    def process(x):
        y = np.zeros_like(x)

        x1 = 0.0
        x2 = 0.0
        y1 = 0.0
        y2 = 0.0

        for i in range(len(x)):
            x0 = x[i]

            y0 = (
                b0 * x0
                + b1 * x1
                + b2 * x2
                - a1 * y1
                - a2 * y2
            )

            y[i] = y0

            x2 = x1
            x1 = x0

            y2 = y1
            y1 = y0

        return y

    # Forward + backward = zero-phase filtering.
    y = process(signal)
    y = process(y[::-1])[::-1]

    return y



audio = butterworth_highpass(
    audio,
    sample_rate,
    HIGH_PASS_CUTOFF
)


# =====================================================
# AFTER HIGH-PASS
# =====================================================

hp_peak = np.max(
    np.abs(audio)
)

hp_rms = np.sqrt(
    np.mean(audio ** 2)
)



# =====================================================
# 3. STFT
# =====================================================

n_fft = 512
hop = 128

window = np.hanning(
    n_fft
).astype(np.float64)

pad = n_fft // 2

x = np.pad(
    audio,
    (pad, pad)
)

n_frames = 1 + (
    (len(x) - n_fft)
    // hop
)

spec = np.empty(
    (
        n_frames,
        n_fft // 2 + 1
    ),
    dtype=np.complex128
)

for i in range(n_frames):

    start = i * hop

    frame = (
        x[
            start:
            start + n_fft
        ]
        *
        window
    )

    spec[i] = np.fft.rfft(
        frame
    )


magnitude = np.abs(
    spec
)

phase = np.angle(
    spec
)


# =====================================================
# 4. FIXED NOISE PROFILE
# =====================================================

noise_frames = max(
    1,
    int(
        (
            NOISE_PROFILE_SECONDS
            * sample_rate
        )
        / hop
    )
)

noise_frames = min(
    noise_frames,
    n_frames
)

noise_profile = np.median(
    magnitude[
        :noise_frames
    ],
    axis=0
)


# =====================================================
# 5. CONSERVATIVE SPECTRAL DENOISE
# =====================================================

mask = (
    magnitude
    -
    OVERSUBTRACTION
    *
    noise_profile[
        None,
        :
    ]
) / (
    magnitude
    +
    1e-8
)

mask = np.clip(
    mask,
    NOISE_FLOOR,
    1.0
)


# =====================================================
# 6. VERY LIGHT FREQUENCY SMOOTHING
# =====================================================

kernel = (
    np.ones(3)
    /
    3.0
)

for i in range(
    n_frames
):

    mask[i] = np.convolve(
        mask[i],
        kernel,
        mode="same"
    )


# =====================================================
# 7. VERY LIGHT TEMPORAL SMOOTHING
# =====================================================

smooth_mask = mask.copy()

for i in range(
    1,
    n_frames
):

    smooth_mask[i] = (
        0.90
        *
        smooth_mask[i - 1]
        +
        0.10
        *
        mask[i]
    )


# =====================================================
# 8. RECONSTRUCT
# =====================================================

clean_spec = (
    magnitude
    *
    smooth_mask
    *
    np.exp(
        1j * phase
    )
)


# =====================================================
# 9. OVERLAP-ADD
# =====================================================

output = np.zeros(
    len(x),
    dtype=np.float64
)

normalization = np.zeros(
    len(x),
    dtype=np.float64
)

for i in range(
    n_frames
):

    frame = np.fft.irfft(
        clean_spec[i],
        n=n_fft
    )

    start = i * hop

    output[
        start:
        start + n_fft
    ] += (
        frame
        *
        window
    )

    normalization[
        start:
        start + n_fft
    ] += (
        window ** 2
    )


output /= np.maximum(
    normalization,
    1e-8
)


# =====================================================
# 10. REMOVE PADDING
# =====================================================

output = output[
    pad:
    pad + len(audio)
]


# =====================================================
# AFTER DENOISE
# =====================================================

denoised_peak = np.max(
    np.abs(output)
)

denoised_rms = np.sqrt(
    np.mean(
        output ** 2
    )
)





# =====================================================
# 11. LIGHT COMPRESSION
# =====================================================

threshold = COMPRESSION_THRESHOLD
ratio = COMPRESSION_RATIO

abs_signal = np.abs(
    output
)

over = np.maximum(
    abs_signal - threshold,
    0
)

compressed_over = (
    over / ratio
)

compressed_abs = (
    np.minimum(
        abs_signal,
        threshold
    )
    +
    compressed_over
)

output = (
    np.sign(output)
    *
    compressed_abs
)


# =====================================================
# AFTER COMPRESSION
# =====================================================

compressed_peak = np.max(
    np.abs(output)
)

compressed_rms = np.sqrt(
    np.mean(
        output ** 2
    )
)





# =====================================================
# 12. FINAL 8X GAIN
# =====================================================


output *= FINAL_GAIN

gain_peak = np.max(
    np.abs(output)
)

gain_rms = np.sqrt(
    np.mean(
        output ** 2
    )
)




# =====================================================
# 13. LIMITER
# =====================================================

LIMIT = 0.98

peak = np.max(
    np.abs(output)
)


LIMIT = 0.98

peak = np.max(
    np.abs(output)
)

if peak > LIMIT:

    limiter_gain = (
        LIMIT / peak
    )

    output *= limiter_gain


# =====================================================
# 14. FINAL SAFETY CLIP
# =====================================================

output = np.clip(
    output,
    -1.0,
    1.0
)



# =====================================================
# 14. FINAL SAFETY CLIP
# =====================================================

output = np.clip(
    output,
    -1.0,
    1.0
)


# =====================================================
# FINAL MEASUREMENTS
# =====================================================

final_peak = np.max(
    np.abs(output)
)

final_rms = np.sqrt(
    np.mean(
        output ** 2
    )
)


# =====================================================
# 15. SAVE
# =====================================================

sf.write(
    OUTPUT_FILE,
    output.astype(np.float32),
    sample_rate,
    subtype="PCM_16"
)


# =====================================================
# FINAL INFORMATION
# =====================================================
















