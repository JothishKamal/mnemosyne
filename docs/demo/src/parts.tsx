import React from "react";
import { AbsoluteFill, interpolate, random, useCurrentFrame } from "remotion";
import { C, F, prog } from "./theme";

export const Backdrop: React.FC<{ glow?: string; intensity?: number }> = ({
  glow = C.sky,
  intensity = 0.12,
}) => {
  const frame = useCurrentFrame();
  const drift = (frame * 0.25) % 64;
  return (
    <AbsoluteFill style={{ background: C.bg }}>
      <AbsoluteFill
        style={{
          backgroundImage: `linear-gradient(${C.border}33 1px, transparent 1px), linear-gradient(90deg, ${C.border}33 1px, transparent 1px)`,
          backgroundSize: "64px 64px",
          backgroundPosition: `${drift}px ${drift}px`,
          maskImage: "radial-gradient(ellipse at center, black 30%, transparent 75%)",
          WebkitMaskImage: "radial-gradient(ellipse at center, black 30%, transparent 75%)",
        }}
      />
      <AbsoluteFill
        style={{
          background: `radial-gradient(ellipse 60% 50% at 50% 45%, ${glow}${Math.round(intensity * 255)
            .toString(16)
            .padStart(2, "0")}, transparent 70%)`,
        }}
      />
    </AbsoluteFill>
  );
};

/** Film grain and vignette, drawn over everything. */
export const Grain: React.FC = () => {
  const frame = useCurrentFrame();
  const seed = Math.floor(frame / 2);
  return (
    <AbsoluteFill style={{ pointerEvents: "none" }}>
      <svg width="100%" height="100%" style={{ opacity: 0.07, mixBlendMode: "screen" }}>
        <filter id={`g${seed}`}>
          <feTurbulence type="fractalNoise" baseFrequency="0.9" numOctaves="2" seed={seed} />
        </filter>
        <rect width="100%" height="100%" filter={`url(#g${seed})`} />
      </svg>
      <AbsoluteFill
        style={{ background: "radial-gradient(ellipse at center, transparent 55%, rgba(0,0,0,0.65) 100%)" }}
      />
    </AbsoluteFill>
  );
};

export const Pill: React.FC<{
  color: string;
  children: React.ReactNode;
  solid?: boolean;
  size?: number;
  style?: React.CSSProperties;
}> = ({ color, children, solid, size = 18, style }) => (
  <span
    style={{
      display: "inline-flex",
      alignItems: "center",
      gap: 8,
      padding: `${size * 0.3}px ${size * 0.8}px`,
      borderRadius: 999,
      fontFamily: F.sans,
      fontWeight: 700,
      fontSize: size,
      letterSpacing: "0.06em",
      textTransform: "uppercase",
      color: solid ? "#1a1000" : color,
      background: solid ? color : `${color}1f`,
      border: `1.5px solid ${color}`,
      whiteSpace: "nowrap",
      ...style,
    }}
  >
    {children}
  </span>
);

/** Text that rises in word by word. */
export const Reveal: React.FC<{
  text: string;
  start: number;
  style?: React.CSSProperties;
  stagger?: number;
}> = ({ text, start, style, stagger = 3 }) => {
  const frame = useCurrentFrame();
  return (
    <span style={{ display: "inline-block", ...style }}>
      {text.split(" ").map((w, i) => {
        const p = prog(frame, start + i * stagger, 18);
        return (
          <span
            key={i}
            style={{
              display: "inline-block",
              opacity: p,
              transform: `translateY(${(1 - p) * 30}px)`,
              filter: `blur(${(1 - p) * 8}px)`,
              marginRight: "0.28em",
            }}
          >
            {w}
          </span>
        );
      })}
    </span>
  );
};

/** Characters appear one by one, with a caret while typing. */
export const Typed: React.FC<{
  text: string;
  start: number;
  cps?: number;
  caret?: boolean;
  style?: React.CSSProperties;
}> = ({ text, start, cps = 40, caret = true, style }) => {
  const frame = useCurrentFrame();
  const n = Math.max(0, Math.floor(((frame - start) / 30) * cps));
  const shown = text.slice(0, n);
  const typing = frame >= start && n < text.length;
  const blink = Math.floor(frame / 15) % 2 === 0;
  return (
    <span style={{ whiteSpace: "pre", ...style }}>
      {shown}
      {caret && frame >= start && (typing || blink) ? (
        <span style={{ opacity: 0.9, color: C.green }}>▍</span>
      ) : null}
    </span>
  );
};

export const ServiceCard: React.FC<{
  name: string;
  x: number;
  y: number;
  appear: number;
  accent?: string;
  status?: React.ReactNode;
  clock?: React.ReactNode;
  scale?: number;
}> = ({ name, x, y, appear, accent = C.sky, status, clock, scale = 1 }) => {
  const frame = useCurrentFrame();
  const p = prog(frame, appear, 20);
  return (
    <div
      style={{
        position: "absolute",
        left: x,
        top: y,
        transform: `translate(-50%, -50%) scale(${(0.85 + 0.15 * p) * scale})`,
        opacity: p,
        width: 300,
        padding: "22px 26px",
        borderRadius: 18,
        background: `linear-gradient(180deg, ${C.panel}, ${C.bg})`,
        border: `2px solid ${accent}`,
        boxShadow: `0 0 50px ${accent}33, inset 0 0 30px ${accent}11`,
        fontFamily: F.sans,
        color: C.fg,
      }}
    >
      <div style={{ fontSize: 14, letterSpacing: "0.18em", color: C.fgFaint, fontWeight: 600 }}>SERVICE</div>
      <div style={{ fontSize: 30, fontWeight: 700, marginTop: 4, fontFamily: F.mono }}>{name}</div>
      {clock ? <div style={{ marginTop: 12, fontFamily: F.mono, fontSize: 18, color: C.fgMuted }}>{clock}</div> : null}
      {status ? <div style={{ marginTop: 14 }}>{status}</div> : null}
    </div>
  );
};

/** A glowing packet moving from (x1,y) to (x2,y) over [start, start+len]. */
export const Packet: React.FC<{
  x1: number;
  x2: number;
  y: number;
  start: number;
  len: number;
  color?: string;
  label?: string;
  hold?: boolean;
}> = ({ x1, x2, y, start, len, color = C.sky, label, hold }) => {
  const frame = useCurrentFrame();
  if (frame < start) return null;
  const p = prog(frame, start, len);
  if (p >= 1 && !hold) {
    const fade = interpolate(frame, [start + len, start + len + 8], [1, 0], { extrapolateRight: "clamp" });
    if (fade <= 0) return null;
  }
  const x = x1 + (x2 - x1) * p;
  const pulse = 1 + 0.25 * Math.sin(frame / 3);
  return (
    <div style={{ position: "absolute", left: x, top: y, transform: "translate(-50%, -50%)" }}>
      <div
        style={{
          width: 22 * (hold && p >= 1 ? pulse : 1),
          height: 22 * (hold && p >= 1 ? pulse : 1),
          borderRadius: 999,
          background: color,
          boxShadow: `0 0 24px ${color}, 0 0 60px ${color}aa`,
        }}
      />
      {label ? (
        <div
          style={{
            position: "absolute",
            top: -44,
            left: "50%",
            transform: "translateX(-50%)",
            fontFamily: F.mono,
            fontSize: 18,
            color,
            whiteSpace: "nowrap",
          }}
        >
          {label}
        </div>
      ) : null}
    </div>
  );
};

/** Deterministic pseudo-random helper for scattered decoration. */
export const rnd = (k: string | number) => random(`mnemo-${k}`);
