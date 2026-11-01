import React from "react";
import { AbsoluteFill, Freeze, interpolate, useCurrentFrame } from "remotion";
import { Backdrop, Packet, Pill, Reveal, ServiceCard, Typed } from "./parts";
import { C, F, prog } from "./theme";

// ---------------------------------------------------------------- cold open

const ALERTS = [
  { at: 18, time: "03:47:12", svc: "checkout-api ", msg: "p99 latency > 30s" },
  { at: 78, time: "03:47:14", svc: "checkout-api ", msg: "5xx rate 38%" },
  { at: 138, time: "03:47:19", svc: "inventory-svc", msg: "retries exhausted calling pricing-svc" },
];

export const ColdOpen: React.FC = () => {
  const frame = useCurrentFrame();
  const out = interpolate(frame, [222, 240], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  const termUp = interpolate(frame, [160, 190], [0, -120], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  return (
    <AbsoluteFill style={{ background: "#000", opacity: out }}>
      <Backdrop glow={C.red} intensity={0.05 + 0.04 * Math.max(0, Math.sin(frame / 9))} />
      <div
        style={{
          position: "absolute",
          left: 240,
          top: 360 + termUp,
          fontFamily: F.mono,
          fontSize: 30,
          lineHeight: 2,
          color: C.fgMuted,
        }}
      >
        {ALERTS.map((a) =>
          frame >= a.at ? (
            <div key={a.at} style={{ opacity: prog(frame, a.at, 6) }}>
              <span style={{ color: C.fgFaint }}>{a.time}  </span>
              <span
                style={{
                  color: "#fff",
                  background: C.red,
                  padding: "2px 10px",
                  borderRadius: 6,
                  fontWeight: 700,
                  boxShadow: frame - a.at < 12 ? `0 0 40px ${C.red}` : "none",
                }}
              >
                ALERT
              </span>
              <span style={{ color: C.fg }}>  {a.svc}  </span>
              <Typed text={a.msg} start={a.at + 4} cps={45} caret={false} />
            </div>
          ) : null,
        )}
      </div>
      <div
        style={{
          position: "absolute",
          width: "100%",
          top: 640,
          textAlign: "center",
          fontFamily: F.sans,
          fontSize: 58,
          fontWeight: 600,
          color: C.fg,
          letterSpacing: "-0.01em",
        }}
      >
        <Reveal text="Somewhere in this system," start={176} />
        <br />
        <Reveal text="a request is still waiting." start={192} style={{ color: C.amber }} />
      </div>
    </AbsoluteFill>
  );
};

// ----------------------------------------------------------------- incident

const XS = [400, 960, 1520];
const Y = 430;

const wall = (frame: number, skewMs: number) => {
  const ms = 14203 + Math.floor((frame / 30) * 1000) + skewMs;
  const s = Math.floor(ms / 1000);
  return `10:34:${String(s).padStart(2, "0")}.${String(ms % 1000).padStart(3, "0")}`;
};

const LOGS = [
  { at: 60, svc: "checkout-api ", msg: "GET /checkout  -> inventory-svc" },
  { at: 92, svc: "inventory-svc", msg: "GET /stock    -> pricing-svc  (attempt 1)" },
  { at: 150, svc: "inventory-svc", msg: "500 from pricing-svc, retrying (attempt 2)" },
  { at: 215, svc: "inventory-svc", msg: "500 from pricing-svc, retrying (attempt 3)" },
];

const Spinner: React.FC<{ color: string }> = ({ color }) => {
  const frame = useCurrentFrame();
  return (
    <span
      style={{
        display: "inline-block",
        width: 22,
        height: 22,
        borderRadius: 999,
        border: `3px solid ${color}33`,
        borderTopColor: color,
        transform: `rotate(${frame * 14}deg)`,
        verticalAlign: "middle",
      }}
    />
  );
};

export const Incident: React.FC = () => {
  const frame = useCurrentFrame();
  const fail1 = frame >= 150;
  const fail2 = frame >= 213;
  const hung = frame >= 245;
  const skewed = frame >= 120;

  const attempt = (n: number, start: number, failAt: number | null) => {
    const failed = failAt !== null && frame >= failAt;
    return (
      <>
        <Packet
          x1={1110}
          x2={1345}
          y={Y}
          start={start}
          len={25}
          color={failAt === null ? C.amber : C.sky}
          hold={!failed}
          label={`attempt ${n}`}
        />
        {failed && frame < failAt! + 40 ? (
          <div
            style={{
              position: "absolute",
              left: 1240,
              top: Y + 52,
              transform: `translate(-50%, -50%) scale(${1 + 0.6 * (1 - prog(frame, failAt!, 10))})`,
              opacity: 1 - prog(frame, failAt! + 25, 15),
              fontFamily: F.sans,
              fontWeight: 800,
              fontSize: 40,
              color: C.red,
              textShadow: `0 0 30px ${C.red}`,
            }}
          >
            ✕ 500
          </div>
        ) : null}
      </>
    );
  };

  const silence = prog(frame, 268, 30);

  return (
    <AbsoluteFill>
      <Backdrop glow={hung ? C.amber : C.sky} intensity={0.1} />
      <div
        style={{
          position: "absolute",
          top: 90,
          width: "100%",
          textAlign: "center",
          fontFamily: F.sans,
          color: C.fgMuted,
          fontSize: 26,
          letterSpacing: "0.2em",
          fontWeight: 600,
        }}
      >
        03:47 AM · INCIDENT IN PROGRESS
      </div>

      {/* wires */}
      {[0, 1].map((i) => (
        <div
          key={i}
          style={{
            position: "absolute",
            left: XS[i] + 150,
            top: Y,
            width: 260 * prog(frame, 10 + i * 8, 20),
            height: 2,
            background: `repeating-linear-gradient(90deg, ${C.border} 0 10px, transparent 10px 18px)`,
          }}
        />
      ))}

      <ServiceCard name="checkout-api" x={XS[0]} y={Y} appear={0} accent={hung ? C.amber : C.sky} clock={wall(frame, 0)} />
      <ServiceCard
        name="inventory-svc"
        x={XS[1]}
        y={Y}
        appear={8}
        accent={hung ? C.amber : C.sky}
        clock={wall(frame, 0)}
        status={
          <span style={{ display: "flex", gap: 8 }}>
            {fail1 ? <Pill color={C.red} size={14}>✕ 1</Pill> : null}
            {fail2 ? <Pill color={C.red} size={14}>✕ 2</Pill> : null}
            {hung ? <Pill color={C.amber} size={14}>… 3</Pill> : null}
          </span>
        }
      />
      <ServiceCard
        name="pricing-svc"
        x={XS[2]}
        y={Y}
        appear={16}
        accent={frame >= 110 ? C.amber : C.sky}
        clock={<span style={{ color: skewed ? C.amber : C.fgMuted }}>{wall(frame, -401)}</span>}
        status={
          frame >= 110 ? (
            <span style={{ fontFamily: F.mono, fontSize: 18, color: C.amber, display: "flex", gap: 12, alignItems: "center" }}>
              <Spinner color={C.amber} /> handler hung
            </span>
          ) : null
        }
      />

      <Packet x1={40} x2={250} y={Y} start={30} len={20} label="GET /checkout" />
      <Packet x1={550} x2={810} y={Y} start={55} len={25} />
      {attempt(1, 85, 150)}
      {attempt(2, 155, 213)}
      {attempt(3, 220, null)}

      {/* logs */}
      <div
        style={{
          position: "absolute",
          left: 250,
          right: 250,
          top: 650,
          height: 300,
          borderRadius: 16,
          background: `${C.panel}cc`,
          border: `1px solid ${C.border}`,
          padding: "22px 30px",
          fontFamily: F.mono,
          fontSize: 22,
          lineHeight: 1.8,
          color: C.fgMuted,
          opacity: prog(frame, 40, 20),
        }}
      >
        <div style={{ color: C.fgFaint, fontSize: 15, letterSpacing: "0.2em", marginBottom: 8 }}>LOGS</div>
        {LOGS.map((l) =>
          frame >= l.at ? (
            <div key={l.at} style={{ opacity: prog(frame, l.at, 6) }}>
              <span style={{ color: C.sky }}>{l.svc}</span>
              {"  "}
              {l.msg}
            </div>
          ) : null,
        )}
        {frame >= 250 ? (
          <div style={{ color: C.fgFaint, opacity: 0.5 + 0.5 * Math.sin(frame / 4) }}>
            (silence)
          </div>
        ) : null}
      </div>

      <AbsoluteFill
        style={{
          background: `rgba(2,6,23,${0.75 * silence})`,
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        {frame >= 268 ? (
          <div style={{ fontFamily: F.sans, fontSize: 64, fontWeight: 700, color: C.fg, textAlign: "center", lineHeight: 1.25 }}>
            <Reveal text="The logs go silent" start={270} />
            <br />
            <Reveal text="exactly where the answer is." start={286} style={{ color: C.amber }} />
          </div>
        ) : null}
      </AbsoluteFill>
    </AbsoluteFill>
  );
};

// ------------------------------------------------------------------ rewind

export const Rewind: React.FC = () => {
  const frame = useCurrentFrame();
  const t = Math.min(1, frame / 104);
  const src = Math.max(0, Math.round(359 * (1 - Math.pow(t, 1.7))));
  const blackout = frame >= 108;
  const jitter = Math.sin(frame * 2.3) * 6;
  const secs = 31 - Math.floor(t * 33);
  const tc = `03:47:${String(Math.max(0, secs)).padStart(2, "0")}`.replace(/:-/, ":");

  return (
    <AbsoluteFill style={{ background: "#000" }}>
      {!blackout ? (
        <>
          <AbsoluteFill style={{ transform: `translateX(${jitter}px)`, filter: "saturate(1.4) contrast(1.15)" }}>
            <Freeze frame={src}>
              <Incident />
            </Freeze>
          </AbsoluteFill>
          {/* chromatic ghost */}
          <AbsoluteFill
            style={{
              transform: `translateX(${-jitter * 2 - 8}px)`,
              mixBlendMode: "screen",
              opacity: 0.35,
              filter: "sepia(1) saturate(6) hue-rotate(-50deg)",
            }}
          >
            <Freeze frame={src}>
              <Incident />
            </Freeze>
          </AbsoluteFill>
          {/* scanlines + tracking bars */}
          <AbsoluteFill
            style={{
              backgroundImage: "repeating-linear-gradient(0deg, rgba(0,0,0,0.35) 0 2px, transparent 2px 4px)",
            }}
          />
          {[0, 1].map((i) => (
            <div
              key={i}
              style={{
                position: "absolute",
                left: 0,
                right: 0,
                top: ((frame * (18 + i * 11) + i * 400) % 1300) - 100,
                height: 40 + i * 30,
                background: "linear-gradient(180deg, transparent, rgba(255,255,255,0.18), transparent)",
                filter: "blur(2px)",
              }}
            />
          ))}
          <div
            style={{
              position: "absolute",
              left: 80,
              top: 70,
              fontFamily: F.mono,
              fontWeight: 700,
              fontSize: 56,
              color: "#fff",
              textShadow: "3px 0 #ff2a6d, -3px 0 #05d9e8",
              opacity: Math.floor(frame / 8) % 2 === 0 ? 1 : 0.6,
            }}
          >
            ◀◀ REWIND
          </div>
          <div
            style={{
              position: "absolute",
              right: 80,
              top: 70,
              fontFamily: F.mono,
              fontWeight: 700,
              fontSize: 56,
              color: "#fff",
              textShadow: "3px 0 #ff2a6d, -3px 0 #05d9e8",
            }}
          >
            {tc}
          </div>
        </>
      ) : null}
    </AbsoluteFill>
  );
};

// ------------------------------------------------------------------- title

const GRAPH: [number, number][] = [
  [260, 220], [520, 160], [700, 330], [430, 420], [1450, 180], [1660, 300], [1250, 330], [1540, 470],
  [300, 760], [560, 880], [1380, 820], [1640, 760], [880, 140], [1060, 930],
];
const EDGES: [number, number][] = [
  [0, 1], [1, 2], [2, 3], [3, 0], [4, 5], [5, 7], [6, 4], [6, 7], [8, 9], [10, 11], [1, 12], [12, 4], [9, 13], [13, 10],
];

export const Title: React.FC = () => {
  const frame = useCurrentFrame();
  const flash = 1 - prog(frame, 0, 14);
  const p = prog(frame, 0, 40);
  const out = interpolate(frame, [168, 180], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  const stages = [
    { label: "record", color: C.sky, at: 74 },
    { label: "store", color: C.sky, at: 88 },
    { label: "replay", color: C.green, at: 102 },
  ];
  return (
    <AbsoluteFill style={{ opacity: out }}>
      <Backdrop glow={C.green} intensity={0.16} />
      <svg width={1920} height={1080} style={{ position: "absolute" }}>
        {EDGES.map(([a, b], i) => {
          const q = prog(frame, 6 + i * 4, 40);
          const [x1, y1] = GRAPH[a];
          const [x2, y2] = GRAPH[b];
          return (
            <line
              key={i}
              x1={x1}
              y1={y1}
              x2={x1 + (x2 - x1) * q}
              y2={y1 + (y2 - y1) * q}
              stroke={C.green}
              strokeOpacity={0.25}
              strokeWidth={2}
            />
          );
        })}
        {GRAPH.map(([x, y], i) => (
          <circle key={i} cx={x} cy={y} r={6 * prog(frame, 4 + i * 3, 20)} fill={C.green} fillOpacity={0.5} />
        ))}
      </svg>
      <AbsoluteFill style={{ justifyContent: "center", alignItems: "center", flexDirection: "column" }}>
        <div
          style={{
            fontFamily: F.sans,
            fontWeight: 800,
            fontSize: 190,
            color: C.fg,
            letterSpacing: `${0.6 - 0.42 * p}em`,
            transform: `scale(${1.12 - 0.12 * p})`,
            textShadow: `0 0 80px ${C.green}66`,
            marginRight: `-${0.6 - 0.42 * p}em`,
          }}
        >
          MNEMOSYNE
        </div>
        <div style={{ fontFamily: F.sans, fontSize: 46, color: C.fgMuted, marginTop: 10 }}>
          <Reveal text="a flight recorder for distributed systems" start={22} stagger={4} />
        </div>
        <div style={{ display: "flex", gap: 26, marginTop: 70, alignItems: "center" }}>
          {stages.map((s, i) => {
            const q = prog(frame, s.at, 16);
            return (
              <React.Fragment key={s.label}>
                {i > 0 ? (
                  <span style={{ color: C.fgFaint, fontSize: 36, opacity: q, fontFamily: F.sans }}>→</span>
                ) : null}
                <span style={{ opacity: q, transform: `translateY(${(1 - q) * 20}px)` }}>
                  <Pill color={s.color} size={26}>
                    {s.label}
                  </Pill>
                </span>
              </React.Fragment>
            );
          })}
        </div>
      </AbsoluteFill>
      <div
        style={{
          position: "absolute",
          bottom: 60,
          width: "100%",
          textAlign: "center",
          fontFamily: F.sans,
          fontSize: 22,
          color: C.fgFaint,
          letterSpacing: "0.3em",
          opacity: prog(frame, 120, 20),
        }}
      >
        ΜΝΗΜΟΣΥΝΗ · TITAN OF MEMORY
      </div>
      <AbsoluteFill style={{ background: "#fff", opacity: flash * 0.9 }} />
    </AbsoluteFill>
  );
};
