import React from "react";
import { AbsoluteFill, Sequence, interpolate, useCurrentFrame } from "remotion";
import { Backdrop, Pill, Reveal } from "./parts";
import { C, F, T, lerp, prog } from "./theme";

const IdeaFrame: React.FC<{
  n: string;
  title: string;
  line: string;
  accent: string;
  badge?: string;
  children: React.ReactNode;
}> = ({ n, title, line, accent, badge, children }) => {
  const frame = useCurrentFrame();
  const inP = prog(frame, 0, 16);
  const out = interpolate(frame, [108, 120], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
  return (
    <AbsoluteFill style={{ opacity: out }}>
      <Backdrop glow={accent} intensity={0.08} />
      <div
        style={{
          position: "absolute",
          left: 120,
          top: 300,
          width: 660,
          fontFamily: F.sans,
          transform: `translateX(${(1 - inP) * -60}px)`,
          opacity: inP,
        }}
      >
        <div style={{ fontFamily: F.mono, fontSize: 30, color: accent, fontWeight: 700, display: "flex", alignItems: "center", gap: 18 }}>
          {n}
          {badge ? (
            <Pill color={C.amber} size={16}>
              {badge}
            </Pill>
          ) : null}
        </div>
        <div style={{ fontSize: 76, fontWeight: 800, color: C.fg, lineHeight: 1.05, marginTop: 14, letterSpacing: "-0.02em" }}>
          {title}
        </div>
        <div style={{ fontSize: 30, color: C.fgMuted, lineHeight: 1.45, marginTop: 30 }}>
          <Reveal text={line} start={10} stagger={1.5} />
        </div>
      </div>
      <div style={{ position: "absolute", left: 0, top: 0, width: 1920, height: 1080, opacity: prog(frame, 4, 14) }}>
        {children}
      </div>
    </AbsoluteFill>
  );
};

const Tag: React.FC<{ x: number; y: number; color: string; children: React.ReactNode; opacity?: number }> = ({
  x,
  y,
  color,
  children,
  opacity = 1,
}) => (
  <div
    style={{
      position: "absolute",
      left: x,
      top: y,
      transform: "translate(-50%, -50%)",
      fontFamily: F.mono,
      fontSize: 15,
      fontWeight: 700,
      color,
      letterSpacing: "0.08em",
      padding: "4px 10px",
      borderRadius: 6,
      background: C.bg,
      border: `1px solid ${color}88`,
      opacity,
      whiteSpace: "nowrap",
    }}
  >
    {children}
  </div>
);

// ------------------------------------------------------- 01 events, not spans

const OPS = [
  { name: "GET /checkout", y: 380, b: 8, bx: 920, e: 46 },
  { name: "GET /stock", y: 540, b: 20, bx: 1000, e: 62 },
  { name: "GET /price", y: 700, b: 32, bx: 1080, e: null as number | null },
];

const EventsNotSpans: React.FC = () => {
  const frame = useCurrentFrame();
  const rate = 9;
  return (
    <IdeaFrame
      n="01"
      title="Events, not spans."
      line="BEGIN is recorded the moment an operation starts. An operation that never ends is still on the record."
      accent={C.amber}
    >
      {OPS.map((o) => {
        if (frame < o.b) return null;
        const endF = o.e ?? Infinity;
        const cur = Math.min(frame, endF);
        const w = Math.min(1780 - o.bx, (cur - o.b) * rate);
        const ended = frame >= endF;
        const color = o.e === null ? C.amber : ended ? C.green : C.sky;
        const pulse = 0.6 + 0.4 * Math.sin(frame / 4);
        return (
          <React.Fragment key={o.name}>
            <div style={{ position: "absolute", left: o.bx, top: o.y - 56, fontFamily: F.mono, fontSize: 22, color: C.fg }}>
              {o.name}
            </div>
            <div
              style={{
                position: "absolute",
                left: o.bx,
                top: o.y - 8,
                width: w,
                height: 16,
                borderRadius: 8,
                background: o.e === null ? `linear-gradient(90deg, ${C.amber}, ${C.amber}${frame > 80 ? "22" : "aa"})` : color,
                boxShadow: `0 0 20px ${color}66`,
              }}
            />
            <Tag x={o.bx} y={o.y + 34} color={C.sky}>
              BEGIN
            </Tag>
            {ended ? (
              <Tag x={o.bx + w} y={o.y + 34} color={C.green} opacity={prog(frame, endF, 6)}>
                END
              </Tag>
            ) : null}
            {o.e === null && frame >= 70 ? (
              <div style={{ position: "absolute", left: 1460, top: o.y + 26, opacity: prog(frame, 70, 10) * (0.7 + 0.3 * pulse) }}>
                <Pill color={C.amber} solid size={20}>
                  never ended
                </Pill>
              </div>
            ) : null}
          </React.Fragment>
        );
      })}
    </IdeaFrame>
  );
};

// ----------------------------------------------------- 02 causality is a DAG

type N = { x: number; y: number; label: string; color?: string };
const NODES: N[] = [
  { x: 1060, y: 250, label: "checkout-api · GET /checkout" },
  { x: 1060, y: 390, label: "checkout-api → /stock" },
  { x: 1060, y: 530, label: "inventory-svc · GET /stock" },
  { x: 920, y: 690, label: "attempt 1" },
  { x: 1140, y: 690, label: "attempt 2" },
  { x: 1360, y: 690, label: "attempt 3", color: C.amber },
  { x: 1600, y: 530, label: "pricing-svc · GET /price" },
  { x: 1600, y: 850, label: "hung-operation", color: C.amber },
];
type E = { a: number; b: number; kind: "PARENT" | "REMOTE_CALL" | "RETRY_OF"; at: number };
const DAG_EDGES: E[] = [
  { a: 0, b: 1, kind: "PARENT", at: 10 },
  { a: 1, b: 2, kind: "REMOTE_CALL", at: 20 },
  { a: 2, b: 3, kind: "PARENT", at: 32 },
  { a: 2, b: 4, kind: "PARENT", at: 36 },
  { a: 2, b: 5, kind: "PARENT", at: 40 },
  { a: 4, b: 3, kind: "RETRY_OF", at: 54 },
  { a: 5, b: 4, kind: "RETRY_OF", at: 62 },
  { a: 5, b: 6, kind: "REMOTE_CALL", at: 74 },
  { a: 6, b: 7, kind: "PARENT", at: 84 },
];
const EDGE_COLOR = { PARENT: C.fgFaint, REMOTE_CALL: C.sky, RETRY_OF: C.green };

const Dag: React.FC = () => {
  const frame = useCurrentFrame();
  const nodeShown = (i: number) =>
    i === 0 ? 4 : Math.min(...DAG_EDGES.filter((e) => e.b === i).map((e) => e.at + 8));
  return (
    <IdeaFrame
      n="02"
      title="Causality is a graph, not a tree."
      line="PARENT for scope. REMOTE_CALL across processes. RETRY_OF between attempts. Every edge was observed; none is inferred."
      accent={C.green}
    >
      <svg width={1920} height={1080} style={{ position: "absolute" }}>
        <defs>
          {Object.entries(EDGE_COLOR).map(([k, c]) => (
            <marker key={k} id={`arrow-${k}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" fill={c} />
            </marker>
          ))}
        </defs>
        {DAG_EDGES.map((e, i) => {
          const A = NODES[e.a];
          const B = NODES[e.b];
          const q = prog(frame, e.at, 14);
          if (q <= 0) return null;
          let d: string;
          if (e.kind === "RETRY_OF") {
            const mx = (A.x + B.x) / 2;
            d = `M${A.x - 50},${A.y + 26} Q${mx},${A.y + 110} ${B.x + 50},${B.y + 26}`;
          } else if (e.a === 5 && e.b === 6) {
            d = `M${A.x + 90},${A.y - 10} L${B.x - 20},${B.y + 26}`;
          } else {
            d = `M${A.x},${A.y + 26} L${B.x},${B.y - 26}`;
          }
          const marker = q > 0.95 ? `url(#arrow-${e.kind})` : undefined;
          return e.kind === "RETRY_OF" ? (
            <path key={i} d={d} fill="none" stroke={EDGE_COLOR[e.kind]} strokeWidth={3} strokeDasharray="10 8" opacity={q} markerEnd={marker} />
          ) : (
            <path
              key={i}
              d={d}
              fill="none"
              stroke={EDGE_COLOR[e.kind]}
              strokeWidth={3}
              pathLength={1}
              strokeDasharray="1 1"
              strokeDashoffset={1 - q}
              markerEnd={marker}
            />
          );
        })}
      </svg>
      {DAG_EDGES.filter((e) => e.kind !== "PARENT").map((e, i) => {
        const A = NODES[e.a];
        const B = NODES[e.b];
        const x = e.kind === "RETRY_OF" ? (A.x + B.x) / 2 : (A.x + B.x) / 2 + (e.a === 5 ? 30 : 90);
        const y = e.kind === "RETRY_OF" ? A.y + 70 : (A.y + B.y) / 2;
        return (
          <Tag key={i} x={x} y={y} color={EDGE_COLOR[e.kind]} opacity={prog(frame, e.at + 8, 8)}>
            {e.kind}
          </Tag>
        );
      })}
      {NODES.map((n, i) => {
        const q = prog(frame, nodeShown(i), 12);
        const color = n.color ?? C.fg;
        return (
          <div
            key={i}
            style={{
              position: "absolute",
              left: n.x,
              top: n.y,
              transform: `translate(-50%, -50%) scale(${0.8 + 0.2 * q})`,
              opacity: q,
              padding: "12px 18px",
              borderRadius: 10,
              background: C.muted,
              borderLeft: `4px solid ${n.color ?? C.green}`,
              fontFamily: F.sans,
              fontSize: 20,
              color,
              whiteSpace: "nowrap",
              boxShadow: n.color ? `0 0 30px ${n.color}44` : "none",
            }}
          >
            {n.label}
          </div>
        );
      })}
    </IdeaFrame>
  );
};

// --------------------------------------------------- 03 hybrid logical clocks

const Hlc: React.FC = () => {
  const frame = useCurrentFrame();
  const swap = prog(frame, 56, 22);
  const rowY = (slot: number) => 430 + slot * 150;
  const sendSlot = lerp(1, 0, swap);
  const recvSlot = lerp(0, 1, swap);
  const mode = swap < 0.5 ? "WALL CLOCK" : "HYBRID LOGICAL CLOCK";
  const bad = frame >= 26 && swap < 0.2;
  const row = (slot: number, svc: string, what: string, wall: string, hlc: string, color: string) => (
    <div
      style={{
        position: "absolute",
        left: 920,
        top: rowY(slot),
        width: 860,
        height: 110,
        transform: "translateY(-50%)",
        display: "flex",
        alignItems: "center",
        gap: 26,
        padding: "0 30px",
        borderRadius: 14,
        background: C.muted,
        borderLeft: `5px solid ${color}`,
        fontFamily: F.sans,
      }}
    >
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 26, color: C.fg, fontWeight: 600 }}>{what}</div>
        <div style={{ fontSize: 19, color: C.fgMuted, fontFamily: F.mono }}>{svc}</div>
      </div>
      <div style={{ fontFamily: F.mono, fontSize: 24, textAlign: "right" }}>
        <div style={{ color: swap < 0.5 ? C.fg : C.fgFaint }}>{wall}</div>
        <div style={{ color: swap >= 0.5 ? C.green : C.fgFaint, fontSize: 20 }}>{hlc}</div>
      </div>
    </div>
  );
  return (
    <IdeaFrame
      n="03"
      title="Hybrid logical clocks."
      line="If A happened before B, hlc(A) < hlc(B), with no clock sync. Skewed machines are flagged, not trusted."
      accent={C.sky}
    >
      <div style={{ position: "absolute", left: 920, top: 270, fontFamily: F.sans, fontSize: 20, letterSpacing: "0.2em", color: C.fgFaint, fontWeight: 700 }}>
        ORDERED BY <span style={{ color: swap >= 0.5 ? C.green : C.red }}>{mode}</span>
      </div>
      {row(sendSlot, "inventory-svc", "send GET /price", "10:34:14.203", "hlc 14203 · 0", C.sky)}
      {row(recvSlot, "pricing-svc (clock −401 ms)", "receive GET /price", "10:34:13.802", "hlc 14203 · 1", C.amber)}
      {bad ? (
        <div
          style={{
            position: "absolute",
            left: 1550,
            top: 505,
            transform: "translate(-50%, -50%)",
            fontFamily: F.sans,
            fontWeight: 800,
            fontSize: 26,
            color: C.red,
            opacity: prog(frame, 26, 8),
            background: C.bg,
            padding: "6px 14px",
            borderRadius: 8,
            border: `1.5px solid ${C.red}`,
          }}
        >
          effect before cause?
        </div>
      ) : null}
      {frame >= 80 ? (
        <div
          style={{
            position: "absolute",
            left: 920,
            top: 760,
            width: 860,
            padding: "18px 24px",
            borderRadius: 10,
            background: "#2b1a0088",
            border: `1px solid ${C.amber}88`,
            borderLeft: `5px solid ${C.amber}`,
            fontFamily: F.sans,
            fontSize: 22,
            color: C.fg,
            opacity: prog(frame, 80, 12),
            transform: `translateY(${(1 - prog(frame, 80, 12)) * 20}px)`,
          }}
        >
          clock skew: pricing-svc is 401ms behind its caller.
        </div>
      ) : null}
    </IdeaFrame>
  );
};

// ------------------------------------------------- 04 retroactive capture

const SVC = [
  { name: "checkout-api", x: 1000, flush: 100 },
  { name: "inventory-svc", x: 1300, flush: 82 },
  { name: "pricing-svc", x: 1600, flush: 56 },
];

const Capture: React.FC = () => {
  const frame = useCurrentFrame();
  const errored = frame >= 52;
  const headerAt = (from: number, to: number, start: number) => {
    const q = prog(frame, start, 14);
    if (frame < start || q >= 1) return null;
    return (
      <div
        style={{
          position: "absolute",
          left: lerp(SVC[from].x, SVC[to].x, q),
          top: 300,
          transform: "translate(-50%, -50%)",
        }}
      >
        <Pill color={C.green} size={15}>
          mnemosyne-capture: 1
        </Pill>
      </div>
    );
  };
  return (
    <IdeaFrame
      n="04"
      title="Capture after the fact."
      line="Payloads wait, redacted, in a per-trace ring buffer, so a failure can flush the whole trace backwards up the call chain."
      accent={C.green}
      badge="being wired up"
    >
      {SVC.map((s, si) => {
        const isErr = si === 2 && errored;
        return (
          <React.Fragment key={s.name}>
            <div
              style={{
                position: "absolute",
                left: s.x,
                top: 380,
                transform: "translate(-50%, -50%)",
                width: 250,
                padding: "14px 0",
                textAlign: "center",
                borderRadius: 12,
                border: `2px solid ${isErr ? C.red : C.border}`,
                background: C.panel,
                fontFamily: F.mono,
                fontSize: 20,
                color: isErr ? C.red : C.fg,
                boxShadow: isErr ? `0 0 40px ${C.red}66` : "none",
              }}
            >
              {s.name}
              {isErr ? <div style={{ fontSize: 15, fontWeight: 700 }}>ERROR</div> : null}
            </div>
            {Array.from({ length: 6 }).map((_, k) => {
              const fill = prog(frame, 4 + k * 6 + si * 3, 8);
              const fly = prog(frame, s.flush + k * 2, 16);
              const col = k % 3;
              const r = Math.floor(k / 3);
              const x0 = s.x - 80 + col * 80;
              const y0 = 470 + r * 70;
              const y = lerp(y0, 860, fly);
              const redacted = k === 1;
              return (
                <div
                  key={k}
                  style={{
                    position: "absolute",
                    left: x0,
                    top: y,
                    transform: `translate(-50%, -50%) scale(${lerp(1, 0.6, fly)})`,
                    width: 68,
                    height: 54,
                    borderRadius: 8,
                    opacity: fill * (1 - prog(frame, s.flush + k * 2 + 14, 4)),
                    background: fly > 0 ? `${C.green}33` : C.muted,
                    border: `1.5px solid ${fly > 0 ? C.green : C.border}`,
                    fontFamily: F.mono,
                    fontSize: 13,
                    color: redacted ? C.fgFaint : C.fgMuted,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {redacted ? "████" : "{…}"}
                </div>
              );
            })}
          </React.Fragment>
        );
      })}
      {headerAt(2, 1, 66)}
      {headerAt(1, 0, 84)}
      <div
        style={{
          position: "absolute",
          left: 870,
          width: 860,
          top: 830,
          height: 64,
          borderRadius: 12,
          border: `2px solid ${C.green}`,
          background: `${C.green}14`,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontFamily: F.sans,
          fontSize: 22,
          color: C.green,
          fontWeight: 600,
        }}
      >
        collector · payloads from before anyone knew they mattered
      </div>
    </IdeaFrame>
  );
};

// ---------------------------------------------------------- 05 honest loss

const Loss: React.FC = () => {
  const frame = useCurrentFrame();
  const down = frame >= 34 && frame < 72;
  const x0 = 920;
  const ticks = Math.min(Math.floor(frame / 1.2), 72);
  const tickX = (i: number) => x0 + i * 12;
  const holeA = Math.floor(34 / 1.2);
  const holeB = Math.floor(72 / 1.2);
  const lane = (y: number, label: string, honest: boolean) => (
    <>
      <div style={{ position: "absolute", left: x0, top: y - 60, fontFamily: F.sans, fontSize: 20, color: honest ? C.green : C.fgFaint, fontWeight: 700, letterSpacing: "0.12em" }}>
        {label}
      </div>
      <div style={{ position: "absolute", left: x0, top: y, width: 870, height: 2, background: C.border }} />
      {Array.from({ length: ticks }).map((_, i) =>
        i >= holeA && i < holeB ? null : (
          <div key={i} style={{ position: "absolute", left: tickX(i), top: y - 18, width: 4, height: 36, borderRadius: 2, background: honest ? C.green : C.fgMuted }} />
        ),
      )}
      {honest && frame >= 74 ? (
        <>
          <div
            style={{
              position: "absolute",
              left: tickX(holeA),
              top: y - 26,
              width: (holeB - holeA) * 12 * prog(frame, 74, 10),
              height: 52,
              borderRadius: 6,
              border: `1.5px solid ${C.red}`,
              background: `repeating-linear-gradient(45deg, ${C.red}55 0 6px, transparent 6px 14px)`,
            }}
          />
          <div style={{ position: "absolute", left: tickX(holeA), top: y + 40, fontFamily: F.sans, fontSize: 22, color: C.red, fontWeight: 700, opacity: prog(frame, 80, 8) }}>
            ⚠ 37 events dropped (UNREACHABLE)
          </div>
        </>
      ) : null}
      {!honest && frame >= 74 ? (
        <div style={{ position: "absolute", left: tickX(holeA), top: y + 40, fontFamily: F.sans, fontSize: 22, color: C.fgFaint, fontStyle: "italic", opacity: prog(frame, 80, 8) }}>
          …looks like nothing happened
        </div>
      ) : null}
    </>
  );
  return (
    <IdeaFrame
      n="05"
      title="Loss that admits it."
      line="The SDK never blocks your app; under pressure it drops. Every drop leaves a gap record, so a hole is never mistaken for silence."
      accent={C.red}
    >
      <div style={{ position: "absolute", left: x0, top: 250, fontFamily: F.mono, fontSize: 22, color: down ? C.red : C.green }}>
        collector {down ? "✕ unreachable" : "● online"}
      </div>
      {lane(450, "A RECORDER WITHOUT GAP RECORDS", false)}
      {lane(720, "MNEMOSYNE", true)}
    </IdeaFrame>
  );
};

export const Ideas: React.FC = () => (
  <>
    {[EventsNotSpans, Dag, Hlc, Capture, Loss].map((Comp, i) => (
      <Sequence key={i} from={i * T.idea} durationInFrames={T.idea}>
        <Comp />
      </Sequence>
    ))}
  </>
);
