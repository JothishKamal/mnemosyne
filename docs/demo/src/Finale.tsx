import React from "react";
import { AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame } from "remotion";
import { Backdrop, Pill, Reveal } from "./parts";
import { C, F, lerp, prog } from "./theme";

// ----------------------------------------------------------- real replay UI

const SW = 1600;
const SH = 900;

type Key = { f: number; s: number; x: number; y: number };
const CAMERA: Key[] = [
  { f: 0, s: 1, x: 800, y: 450 },
  { f: 30, s: 1, x: 800, y: 450 },
  { f: 70, s: 1.75, x: 420, y: 150 },
  { f: 100, s: 1.75, x: 420, y: 150 },
  { f: 128, s: 1.25, x: 520, y: 450 },
  { f: 160, s: 1.25, x: 520, y: 450 },
  { f: 195, s: 1.6, x: 1310, y: 360 },
  { f: 262, s: 1.6, x: 1310, y: 420 },
  { f: 290, s: 1, x: 800, y: 450 },
];

const cam = (frame: number) => {
  let i = 0;
  while (i < CAMERA.length - 2 && frame >= CAMERA[i + 1].f) i++;
  const a = CAMERA[i];
  const b = CAMERA[i + 1];
  const t = prog(frame, a.f, b.f - a.f);
  return { s: lerp(a.s, b.s, t), x: lerp(a.x, b.x, t), y: lerp(a.y, b.y, t) };
};

export const Replay: React.FC = () => {
  const frame = useCurrentFrame();
  const inP = prog(frame, 0, 24);
  const out = interpolate(frame, [286, 300], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  const { s, x, y } = cam(frame);
  // Keep the screenshot covering the viewport at every zoom level.
  const tx = Math.min(0, Math.max(SW - SW * s, SW / 2 - x * s));
  const ty = Math.min(0, Math.max(SH - SH * s, SH / 2 - y * s));
  const chainFade = prog(frame, 138, 10);

  // Cursor path, in screenshot coordinates.
  const cur = prog(frame, 104, 26);
  const cx = lerp(900, 125, cur);
  const cy = lerp(650, 430, cur);
  const clickR = frame >= 132 ? prog(frame, 132, 14) : 0;

  // Highlight sweeping down the causal chain.
  const chainY = [126, 183, 246, 309, 367, 419, 471, 523, 575];
  const sweep = Math.min(chainY.length - 1, Math.max(0, Math.floor((frame - 170) / 9)));

  return (
    <AbsoluteFill style={{ opacity: out }}>
      <Backdrop glow={C.green} intensity={0.08} />
      <div style={{ position: "absolute", top: 46, width: "100%", textAlign: "center", fontFamily: F.sans, fontSize: 52, fontWeight: 800, color: C.fg }}>
        <Reveal text="Then replay exactly what happened." start={4} />
      </div>
      <div
        style={{
          position: "absolute",
          left: (1920 - 1500) / 2,
          top: 150,
          width: 1500,
          borderRadius: 14,
          overflow: "hidden",
          border: `1px solid ${C.border}`,
          boxShadow: `0 40px 120px #000, 0 0 80px ${C.green}22`,
          transform: `translateY(${(1 - inP) * 80}px)`,
          opacity: inP,
        }}
      >
        <div style={{ height: 44, background: "#0b0f1d", display: "flex", alignItems: "center", gap: 10, padding: "0 16px" }}>
          {["#ff5f57", "#febc2e", "#28c840"].map((c) => (
            <span key={c} style={{ width: 13, height: 13, borderRadius: 99, background: c }} />
          ))}
          <span
            style={{
              marginLeft: 18,
              padding: "5px 18px",
              borderRadius: 8,
              background: C.muted,
              fontFamily: F.mono,
              fontSize: 15,
              color: C.fgMuted,
            }}
          >
            localhost:4318
          </span>
        </div>
        <div style={{ width: 1500, height: 1500 * (SH / SW), overflow: "hidden", position: "relative" }}>
          <div
            style={{
              width: SW,
              height: SH,
              transformOrigin: "0 0",
              transform: `scale(${1500 / SW}) translate(${tx}px, ${ty}px) scale(${s})`,
              position: "relative",
            }}
          >
            <Img src={staticFile("ui-loaded.png")} style={{ position: "absolute", width: SW, height: SH }} />
            <Img src={staticFile("ui-chain.png")} style={{ position: "absolute", width: SW, height: SH, opacity: chainFade }} />
            {/* skew banner highlight */}
            {frame >= 60 && frame < 115 ? (
              <div
                style={{
                  position: "absolute",
                  left: 14,
                  top: 70,
                  width: 1172,
                  height: 42,
                  borderRadius: 6,
                  border: `2px solid ${C.amber}`,
                  boxShadow: `0 0 30px ${C.amber}`,
                  opacity: prog(frame, 60, 8) * (1 - prog(frame, 105, 10)),
                }}
              />
            ) : null}
            {/* sweeping chain highlight */}
            {frame >= 168 && frame < 280 ? (
              <div
                style={{
                  position: "absolute",
                  left: 1214,
                  top: chainY[sweep] - 24,
                  width: 372,
                  height: 48,
                  borderRadius: 6,
                  border: `2px solid ${C.green}`,
                  boxShadow: `0 0 26px ${C.green}`,
                  opacity: 1 - prog(frame, 265, 12),
                }}
              />
            ) : null}
            {/* cursor */}
            {frame >= 100 && frame < 165 ? (
              <>
                {clickR > 0 && clickR < 1 ? (
                  <div
                    style={{
                      position: "absolute",
                      left: 125 - 40 * clickR,
                      top: 430 - 40 * clickR,
                      width: 80 * clickR,
                      height: 80 * clickR,
                      borderRadius: 999,
                      border: `3px solid ${C.amber}`,
                      opacity: 1 - clickR,
                    }}
                  />
                ) : null}
                <svg style={{ position: "absolute", left: cx, top: cy }} width={28} height={36} viewBox="0 0 28 36">
                  <path d="M2 2 L2 30 L9 23 L14 34 L19 32 L14 21 L24 21 Z" fill="#fff" stroke="#000" strokeWidth={2} />
                </svg>
              </>
            ) : null}
          </div>
        </div>
      </div>
      {/* callouts */}
      {frame >= 66 && frame < 112 ? (
        <div style={{ position: "absolute", left: 300, bottom: 70, opacity: prog(frame, 66, 10) * (1 - prog(frame, 104, 8)) }}>
          <Pill color={C.amber} size={24}>clock skew detected, not hidden</Pill>
        </div>
      ) : null}
      {frame >= 200 && frame < 285 ? (
        <div style={{ position: "absolute", left: 300, bottom: 70, opacity: prog(frame, 200, 10) * (1 - prog(frame, 276, 8)) }}>
          <Pill color={C.green} size={24}>9-node causal chain · every edge recorded</Pill>
        </div>
      ) : null}
    </AbsoluteFill>
  );
};

// --------------------------------------------------------------------- code

const CODE = `ex := exporter.New(exporter.Config{Endpoint: "http://collector:4318"})
rec, _ := sdk.New(sdk.Config{Service: "checkout-api", Sink: ex, Enabled: true})

// inbound: join the caller's trace
handler := mnemohttp.Middleware(rec)(mux)

// outbound: propagate traceparent + tracestate
client := &http.Client{Transport: mnemohttp.Transport(rec, nil)}`;

type Tok = { t: string; c: string };
const tokenize = (src: string): Tok[] => {
  const out: Tok[] = [];
  const re = /(\/\/[^\n]*)|("[^"]*")|(\b(?:true|nil|func|return)\b)|(:=|&)|(\.[A-Za-z]+)|([A-Za-z_]+(?=:))|([\s\S])/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(src))) {
    const [s, com, str, kw, op, sel, field] = m;
    const c = com ? C.fgFaint : str ? C.green : kw ? C.violet : op ? C.amber : sel ? C.sky : field ? "#cbd5e1" : C.fg;
    const last = out[out.length - 1];
    if (last && last.c === c && !com && !str) last.t += s;
    else out.push({ t: s, c });
  }
  return out;
};
const TOKENS = tokenize(CODE);

export const Code: React.FC = () => {
  const frame = useCurrentFrame();
  const n = Math.floor(Math.max(0, frame - 14) * 3.2);
  const out = interpolate(frame, [168, 180], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  let left = n;
  const shown = TOKENS.map((tk, i) => {
    if (left <= 0) return null;
    const part = tk.t.slice(0, left);
    left -= tk.t.length;
    return (
      <span key={i} style={{ color: tk.c }}>
        {part}
      </span>
    );
  });
  const termAt = 120;
  return (
    <AbsoluteFill style={{ opacity: out }}>
      <Backdrop glow={C.violet} intensity={0.07} />
      <div style={{ position: "absolute", top: 60, width: "100%", textAlign: "center", fontFamily: F.sans, fontSize: 52, fontWeight: 800, color: C.fg }}>
        <Reveal text="A few lines to instrument. One binary to run." start={0} stagger={2} />
      </div>
      <div
        style={{
          position: "absolute",
          left: 290,
          top: 180,
          width: 1340,
          borderRadius: 14,
          background: C.panel,
          border: `1px solid ${C.border}`,
          boxShadow: "0 30px 90px #000",
          opacity: prog(frame, 0, 14),
        }}
      >
        <div style={{ padding: "12px 20px", borderBottom: `1px solid ${C.border}`, fontFamily: F.mono, fontSize: 16, color: C.fgFaint }}>
          main.go
        </div>
        <pre style={{ margin: 0, padding: "24px 30px", fontFamily: F.mono, fontSize: 25, lineHeight: 1.6, minHeight: 360 }}>
          {shown}
          {Math.floor(frame / 12) % 2 === 0 || n < CODE.length ? <span style={{ color: C.green }}>▍</span> : null}
        </pre>
      </div>
      <div
        style={{
          position: "absolute",
          left: 290,
          top: 640,
          width: 1340,
          borderRadius: 14,
          background: "#000",
          border: `1px solid ${C.border}`,
          padding: "22px 30px",
          fontFamily: F.mono,
          fontSize: 24,
          lineHeight: 1.7,
          opacity: prog(frame, termAt - 10, 10),
        }}
      >
        <div>
          <span style={{ color: C.green }}>$</span> <span style={{ color: C.fg }}>go run ./cmd/mnemo</span>
        </div>
        {frame >= termAt + 14 ? (
          <div style={{ color: C.fgMuted }}>mnemo collector listening on :4318 (store=sqlite)</div>
        ) : null}
        {frame >= termAt + 26 ? (
          <div>
            <span style={{ color: C.green }}>$</span> <span style={{ color: C.fg }}>open http://localhost:4318</span>
          </div>
        ) : null}
      </div>
    </AbsoluteFill>
  );
};

// -------------------------------------------------------------------- outro

export const Outro: React.FC = () => {
  const frame = useCurrentFrame();
  const flash = 1 - prog(frame, 0, 16);
  const lift = prog(frame, 84, 30);
  const fade = interpolate(frame, [200, 236], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  const chips = ["Go SDK", "net/http", "HLC ordering", "SQLite + blobs", "replay UI"];
  return (
    <AbsoluteFill style={{ opacity: fade }}>
      <Backdrop glow={C.green} intensity={0.14} />
      <AbsoluteFill style={{ justifyContent: "center", alignItems: "center", flexDirection: "column" }}>
        <div
          style={{
            fontFamily: F.sans,
            fontSize: 74,
            fontWeight: 700,
            color: C.fg,
            textAlign: "center",
            lineHeight: 1.2,
            transform: `translateY(${-lift * 150}px) scale(${1 - lift * 0.3})`,
          }}
        >
          <Reveal text="Record execution," start={6} />
          <br />
          <Reveal text="not interpretation." start={22} style={{ color: C.green }} />
        </div>
        <div
          style={{
            position: "absolute",
            top: 560,
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            opacity: lift,
            transform: `translateY(${(1 - lift) * 40}px)`,
          }}
        >
          <div style={{ fontFamily: F.sans, fontWeight: 800, fontSize: 96, letterSpacing: "0.18em", marginRight: "-0.18em", color: C.fg, textShadow: `0 0 60px ${C.green}55` }}>
            MNEMOSYNE
          </div>
          <div style={{ display: "flex", gap: 16, marginTop: 30 }}>
            {chips.map((c, i) => (
              <span key={c} style={{ opacity: prog(frame, 104 + i * 5, 12) }}>
                <Pill color={i === 4 ? C.green : C.sky} size={19}>
                  {c}
                </Pill>
              </span>
            ))}
          </div>
          <div style={{ marginTop: 40, fontFamily: F.mono, fontSize: 28, color: C.fgMuted, opacity: prog(frame, 136, 14) }}>
            github.com/JothishKamal/mnemosyne
          </div>
        </div>
      </AbsoluteFill>
      <AbsoluteFill style={{ background: "#fff", opacity: flash * 0.8 }} />
    </AbsoluteFill>
  );
};
