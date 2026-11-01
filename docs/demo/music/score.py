"""Original score for the Mnemosyne demo video, synthesized from scratch.

No samples, no external assets: every sound below is built from oscillators
and filtered noise, so the track carries no licensing baggage. The arrangement
is locked to the video's scene timeline (see src/timeline.ts): 120 BPM, one
bar = 2 s = 60 frames at 30 fps, so every scene boundary lands on a downbeat.

    0-8    cold open     drone, heartbeat, alert blips
    8-20   incident      groove builds, riser into the rewind
    20-24  rewind        the last 8 s of music played backwards, accelerating
    24-50  title, ideas  impact, full groove
    50-66  replay, code  groove, lighter top end
    66-74  outro         impact, held chord, fade

Usage: python score.py out.wav
"""

import sys
import wave

import numpy as np
from scipy.signal import butter, fftconvolve, lfilter, sosfilt

SR = 44100
BPM = 120
BEAT = 60 / BPM
BAR = 4 * BEAT
DUR = 74.0
N = int(DUR * SR)
rng = np.random.default_rng(7)

A2 = 110.0


def hz(semitones_from_a2):
    return A2 * 2 ** (semitones_from_a2 / 12)


# Am, F, C, G as semitone offsets from A2 (root, third, fifth).
CHORDS = [(0, 3, 7), (-4, 0, 3), (3, 7, 10), (-2, 2, 5)]


def chord_at(t):
    return CHORDS[int(t // BAR) % 4]


def t_axis(n):
    return np.arange(n) / SR


def saw(f, n, phase=0.0):
    p = (phase + f * t_axis(n)) % 1.0
    return 2 * p - 1


def lowpass(x, cutoff, order=2):
    sos = butter(order, min(cutoff, SR / 2 - 100), "low", fs=SR, output="sos")
    return sosfilt(sos, x)


def highpass(x, cutoff, order=2):
    sos = butter(order, cutoff, "high", fs=SR, output="sos")
    return sosfilt(sos, x)


def bandpass(x, lo, hi, order=2):
    sos = butter(order, [lo, hi], "band", fs=SR, output="sos")
    return sosfilt(sos, x)


def place(buf, sig, at):
    i = int(at * SR)
    if i >= len(buf):
        return
    j = min(len(buf), i + len(sig))
    buf[i:j] += sig[: j - i]


def env_exp(n, decay):
    return np.exp(-t_axis(n) / decay)


def env_adsr(n, a, r):
    e = np.ones(n)
    na, nr = int(a * SR), int(r * SR)
    e[:na] = np.linspace(0, 1, na)
    if nr:
        e[-nr:] *= np.linspace(1, 0, nr)
    return e


# ---------------------------------------------------------------- voices ---


def kick():
    n = int(0.45 * SR)
    t = t_axis(n)
    f = 45 + 110 * np.exp(-t / 0.035)
    ph = 2 * np.pi * np.cumsum(f) / SR
    click = highpass(rng.standard_normal(n), 3000) * env_exp(n, 0.003) * 0.3
    return (np.sin(ph) * env_exp(n, 0.16) + click) * 0.9


def clap():
    n = int(0.3 * SR)
    noise = bandpass(rng.standard_normal(n), 900, 5000)
    e = np.zeros(n)
    for k, off in enumerate((0, 0.011, 0.022)):
        i = int(off * SR)
        e[i:] += env_exp(n - i, 0.012 if k < 2 else 0.09)
    return noise * e * 0.45


def hat(open_=False):
    n = int((0.25 if open_ else 0.06) * SR)
    x = highpass(rng.standard_normal(n), 7000, 4)
    return x * env_exp(n, 0.07 if open_ else 0.014) * 0.22


def pluck(f, length=0.22, bright=3500):
    n = int(length * SR)
    x = saw(f, n) + 0.5 * saw(f * 1.005, n)
    x = lowpass(x, bright)
    return x * env_exp(n, length / 3.5) * 0.18


def pad_note(f, n, cutoff):
    x = sum(saw(f * d, n, rng.random()) for d in (0.994, 1.0, 1.006))
    return lowpass(x / 3, cutoff, 2)


def sub_boom():
    n = int(2.6 * SR)
    t = t_axis(n)
    f = 38 + 60 * np.exp(-t / 0.08)
    ph = 2 * np.pi * np.cumsum(f) / SR
    body = np.sin(ph) * env_exp(n, 0.9)
    crash = lowpass(rng.standard_normal(n), 6000) * env_exp(n, 0.6) * 0.35
    return (body + crash) * 0.95


def whoosh(length=0.6):
    n = int(length * SR)
    x = rng.standard_normal(n)
    out = np.zeros(n)
    # Sweep a band upward by filtering short overlapping chunks.
    steps = 24
    seg = n // steps
    for s in range(steps):
        lo = 300 * 2 ** (s / steps * 4)
        chunk = bandpass(x[s * seg:(s + 1) * seg + seg], lo, min(lo * 2.2, 18000))
        w = np.hanning(len(chunk))
        out[s * seg:s * seg + len(chunk)] += (chunk * w)[: n - s * seg]
    return out * np.linspace(0, 1, n) ** 2 * 0.25


def blip(f=1760, length=0.12):
    n = int(length * SR)
    return np.sin(2 * np.pi * f * t_axis(n)) * env_exp(n, 0.04) * 0.22


# ------------------------------------------------------------ arrangement ---

drums = np.zeros(N)
bass = np.zeros(N)
pad = np.zeros(N)
arp = np.zeros(N)
fx = np.zeros(N)
duck = np.ones(N)  # sidechain gain applied to bass and pad

K, C = kick(), clap()


def kick_at(t, gain=1.0):
    place(drums, K * gain, t)
    n = int(0.3 * SR)
    i = int(t * SR)
    j = min(N, i + n)
    duck[i:j] = np.minimum(duck[i:j], 1 - 0.6 * env_exp(n, 0.09)[: j - i])


# Cold open: heartbeat thumps and alert blips over a drone.
for bar in range(4):
    t0 = bar * BAR
    kick_at(t0, 0.55)
    kick_at(t0 + 0.28, 0.35)
for t in (0.6, 0.9, 2.6, 2.9, 4.6, 4.9):
    place(fx, blip(), t)

# Incident 8-20: four-on-the-floor arrives, hats at 12, arp at 16, riser.
for b in range(int(8 / BEAT), int(20 / BEAT)):
    t = b * BEAT
    kick_at(t, 0.8)
    if t >= 12:
        place(drums, hat(), t + BEAT / 2)
    if t >= 16 and b % 2 == 1:
        place(drums, C * 0.7, t)

# Groove 24-66, with a breakdown feel in the code section.
for b in range(int(24 / BEAT), int(66 / BEAT)):
    t = b * BEAT
    kick_at(t)
    if b % 2 == 1 and not (60 <= t < 66):
        place(drums, C, t)
    place(drums, hat(open_=(b % 4 == 3)), t + BEAT / 2)
    if 30 <= t < 60:
        place(drums, hat() * 0.5, t + BEAT / 4)
        place(drums, hat() * 0.5, t + 3 * BEAT / 4)

# Bass: eighth notes on the chord root.
for i in range(int(8 / (BEAT / 2)), int(70 / (BEAT / 2))):
    t = i * BEAT / 2
    if 20 <= t < 24:
        continue
    root = chord_at(t)[0] - 12
    n = int(BEAT / 2 * SR)
    x = saw(hz(root), n) + 0.6 * np.sin(2 * np.pi * hz(root) * t_axis(n))
    cutoff = 500 if t < 24 else 900
    place(bass, lowpass(x, cutoff) * env_adsr(n, 0.004, 0.05) * 0.32, t)

# Pad: one chord per bar for the whole piece; opens up at the drop.
for bar in range(int(DUR / BAR)):
    t0 = bar * BAR
    if 20 <= t0 < 24:
        continue
    n = int(BAR * SR) + int(0.4 * SR)
    cutoff = 700 if t0 < 8 else 1100 if t0 < 24 else 2200 if t0 < 66 else 1400
    sig = sum(pad_note(hz(s + 12), n, cutoff) for s in chord_at(t0))
    place(pad, sig * env_adsr(n, 0.25, 0.4) * 0.1, t0)

# Drone under the cold open.
n = int(8 * SR)
drone = lowpass(saw(hz(-12), n) + saw(hz(-12) * 1.004, n), 300) * 0.18
place(pad, drone * env_adsr(n, 2.0, 0.5), 0)

# Arp: sixteenth notes over chord tones, up two octaves.
pattern = [0, 1, 2, 1, 2, 0, 1, 2]
for i in range(int(16 / (BEAT / 4)), int(72 / (BEAT / 4))):
    t = i * BEAT / 4
    if 20 <= t < 24:
        continue
    if t >= 66 and i % 2:
        continue  # thin out in the outro
    tones = chord_at(t)
    note = tones[pattern[i % 8]] + 24
    bright = 2500 if t < 24 else 4500
    gain = 0.7 if t < 24 else 1.0
    place(arp, pluck(hz(note), bright=bright) * gain, t)

# Riser into the rewind.
n = int(4 * SR)
riser = whoosh(4.0) * 1.6
riser += np.sin(2 * np.pi * np.cumsum(np.linspace(200, 1200, n)) / SR) * np.linspace(0, 0.08, n)
place(fx, riser, 16)

# Impacts and transition whooshes.
for t in (24, 66):
    place(fx, sub_boom(), t)
for t in (30, 34, 38, 42, 46, 50, 60):
    place(fx, whoosh(0.6), t - 0.6)

# ------------------------------------------------------------------ mix ---


def reverb(x, seconds=2.4, seed=1):
    n = int(seconds * SR)
    r = np.random.default_rng(seed)
    ir = r.standard_normal(n) * np.exp(-t_axis(n) / (seconds / 5))
    ir = lowpass(ir, 6000)
    ir /= np.sqrt(np.sum(ir ** 2))
    return fftconvolve(x, ir)[: len(x)]


def delay(x, seconds, feedback=0.35, mix=0.3):
    d = int(seconds * SR)
    b = np.zeros(d + 1)
    b[0] = 1
    a = np.zeros(d + 1)
    a[0], a[d] = 1, -feedback
    return x + mix * (lfilter(b, a, x) - x)


bass *= duck
pad *= 0.55 + 0.45 * duck

left = drums + bass + pad + delay(arp, BEAT * 0.75) + fx
right = drums + bass + pad + delay(arp, BEAT * 0.5) + fx
send = pad + arp * 0.6 + fx * 0.5 + drums * 0.08
left += 0.35 * reverb(send, seed=1)
right += 0.35 * reverb(send, seed=2)
mix = np.stack([left, right])

# Rewind 20-24: the previous 8 s played backwards, accelerating from 1x to
# 4x, ending in a tape-stop gulp, then a breath before the impact at 24.
src = mix[:, int(12 * SR):int(20 * SR)][:, ::-1]
out_len = int(3.6 * SR)
speed = np.linspace(1.0, 4.0, out_len) ** 1.5
pos = np.cumsum(speed)
pos = pos / pos[-1] * (src.shape[1] - 1)
rew = np.stack([np.interp(pos, np.arange(src.shape[1]), ch) for ch in src])
rew = np.stack([lowpass(ch, 5000) for ch in rew])
rew *= np.linspace(0.9, 0.5, out_len) * np.concatenate(
    [np.ones(out_len - int(0.08 * SR)), np.linspace(1, 0, int(0.08 * SR))]
)
s20, s24 = int(20 * SR), int(24 * SR)
mix[:, s20:s24] = 0
mix[:, s20:s20 + out_len] = rew
# A reversed cymbal swelling into the impact.
n = int(1.2 * SR)
rev_cym = (highpass(rng.standard_normal(n), 4000) * env_exp(n, 0.35))[::-1] * 0.3
mix[:, s24 - n:s24] += rev_cym

# Final fade and master.
fade = int(3.0 * SR)
mix[:, -fade:] *= np.linspace(1, 0, fade) ** 1.5
mix /= np.max(np.abs(mix)) + 1e-9
mix = np.tanh(mix * 1.6) / np.tanh(1.6)
mix *= 10 ** (-1 / 20)

pcm = (mix.T * 32767).astype("<i2")
with wave.open(sys.argv[1] if len(sys.argv) > 1 else "score.wav", "wb") as w:
    w.setnchannels(2)
    w.setsampwidth(2)
    w.setframerate(SR)
    w.writeframes(pcm.tobytes())
print(f"wrote {DUR:.0f}s score")
