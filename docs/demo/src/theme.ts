import { loadFont as loadInter } from "@remotion/google-fonts/Inter";
import { loadFont as loadMono } from "@remotion/google-fonts/JetBrainsMono";
import { Easing, interpolate } from "remotion";

const inter = loadInter("normal", { weights: ["400", "500", "600", "700", "800"], subsets: ["latin"] });
const mono = loadMono("normal", { weights: ["400", "500", "700"], subsets: ["latin"] });

export const FPS = 30;

// The same palette the replay UI ships with (ui/style.css), so the video and
// the product read as one thing.
export const C = {
  bg: "#020617",
  panel: "#0e1223",
  muted: "#1a1e2f",
  border: "#26314a",
  fg: "#f8fafc",
  fgMuted: "#94a3b8",
  fgFaint: "#64748b",
  green: "#22c55e",
  red: "#ef4444",
  amber: "#f59e0b",
  sky: "#38bdf8",
  violet: "#a78bfa",
};

export const F = {
  sans: `${inter.fontFamily}, "Segoe UI", sans-serif`,
  mono: `${mono.fontFamily}, Consolas, monospace`,
};

/** Scene start frames. One bar of the score is 60 frames (120 BPM). */
export const T = {
  cold: 0,
  incident: 240,
  rewind: 600,
  title: 720,
  ideas: 900,
  idea: 120,
  replay: 1500,
  code: 1800,
  outro: 1980,
  end: 2220,
};

export const ease = Easing.bezier(0.22, 1, 0.36, 1);

/** 0 to 1 over [start, start + len], eased and clamped. */
export const prog = (frame: number, start: number, len: number, e = ease) =>
  interpolate(frame, [start, start + len], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: e,
  });

export const lerp = (a: number, b: number, t: number) => a + (b - a) * t;
