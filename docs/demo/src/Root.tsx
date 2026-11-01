import React from "react";
import { AbsoluteFill, Audio, Composition, Sequence, staticFile } from "remotion";
import { Code, Outro, Replay } from "./Finale";
import { Ideas } from "./Ideas";
import { ColdOpen, Incident, Rewind, Title } from "./Opening";
import { Grain } from "./parts";
import { C, FPS, T } from "./theme";

const Scenes: React.FC = () => (
  <>
    <Sequence from={T.cold} durationInFrames={T.incident - T.cold}>
      <ColdOpen />
    </Sequence>
    <Sequence from={T.incident} durationInFrames={T.rewind - T.incident}>
      <Incident />
    </Sequence>
    <Sequence from={T.rewind} durationInFrames={T.title - T.rewind}>
      <Rewind />
    </Sequence>
    <Sequence from={T.title} durationInFrames={T.ideas - T.title}>
      <Title />
    </Sequence>
    <Sequence from={T.ideas} durationInFrames={T.replay - T.ideas}>
      <Ideas />
    </Sequence>
    <Sequence from={T.replay} durationInFrames={T.code - T.replay}>
      <Replay />
    </Sequence>
    <Sequence from={T.code} durationInFrames={T.outro - T.code}>
      <Code />
    </Sequence>
    <Sequence from={T.outro} durationInFrames={T.end - T.outro}>
      <Outro />
    </Sequence>
  </>
);

export const Demo: React.FC = () => (
  <AbsoluteFill style={{ background: C.bg }}>
    <Scenes />
    <Grain />
    <Audio src={staticFile("score.wav")} />
  </AbsoluteFill>
);

/** A silent, grain-free loop for the README preview: rewind, title, the DAG. */
export const Teaser: React.FC = () => (
  <AbsoluteFill style={{ background: C.bg }}>
    <Sequence durationInFrames={120}>
      <Rewind />
    </Sequence>
    <Sequence from={120} durationInFrames={180}>
      <Title />
    </Sequence>
  </AbsoluteFill>
);

export const Root: React.FC = () => (
  <>
    <Composition id="Demo" component={Demo} durationInFrames={T.end} fps={FPS} width={1920} height={1080} />
    <Composition id="Teaser" component={Teaser} durationInFrames={300} fps={FPS} width={1920} height={1080} />
  </>
);
