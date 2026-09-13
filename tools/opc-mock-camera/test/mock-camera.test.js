import assert from "node:assert/strict";
import dgram from "node:dgram";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { once } from "node:events";
import test from "node:test";
import {
  DumlStreamDecoder,
  FLAG_REQUEST,
  FLAG_RESPONSE,
  PKT_HANDSHAKE,
  PKT_TELEMETRY,
  ProtocolError,
  decodeDuml,
  decodeTransport,
  encodeDuml,
  ackDatagram,
  handshakeDatagram,
  parseSubscription,
  scanFrames,
  transportHeader,
  wrapCommand,
} from "../protocol.js";
import { MockCameraState, mediaManifest } from "../state.js";
import { MockCameraServer, parseArgs } from "../server.js";
import * as publicApi from "../index.js";

function frame(overrides = {}) {
  return {
    sender: 0x02,
    receiver: 0x01,
    seq: 0x1234,
    flags: FLAG_REQUEST,
    cmdSet: 0x02,
    cmdId: 0x02,
    payload: Buffer.from([0]),
    ...overrides,
  };
}

function subscriptionPayload(name, subId) {
  const nameBytes = Buffer.from(name, "ascii");
  return Buffer.concat([
    Buffer.from([0x02, 0x02, 0x00, 0x00]),
    u32(subId),
    Buffer.alloc(3),
    u16(nameBytes.length + 6),
    u16(nameBytes.length),
    nameBytes,
    Buffer.alloc(4),
  ]);
}

function listPayload(counter, cursor) {
  const payload = Buffer.alloc(42);
  payload[0] = 0x4a;
  payload[2] = 0x2a;
  payload[3] = 0x10;
  payload[4] = counter;
  payload[10] = cursor & 0xff;
  payload[11] = (cursor >>> 8) & 0xff;
  payload[12] = (cursor >>> 16) & 0xff;
  payload[13] = (cursor >>> 24) & 0xff;
  payload[14] = 0x2d;
  payload[16] = 0x0d;
  payload[17] = 0x01;
  return payload;
}

function u16(value) {
  const out = Buffer.alloc(2);
  out.writeUInt16LE(value);
  return out;
}

function u32(value) {
  const out = Buffer.alloc(4);
  out.writeUInt32LE(value >>> 0);
  return out;
}

function registerPayload() {
  const payload = Buffer.alloc(62);
  payload[1] = 0x41;
  payload[2] = 0x50;
  payload[3] = 0x50;
  payload[41] = 0x02;
  payload[50] = 0x02;
  payload[51] = 0x08;
  return payload;
}

function presencePayload() {
  return Buffer.from([0x17, 0x00, 0x46, 0x23, 0x7c, 0x41, 0x50, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]);
}

function pairingPayload() {
  const identifier = Buffer.from("284ae5b8d76b3375a04a6417ad71bea3", "ascii");
  const pin = Buffer.from("mock", "ascii");
  return Buffer.concat([Buffer.from([identifier.length]), identifier, Buffer.from([pin.length]), pin]);
}

class UdpCollector {
  constructor(socket) {
    this.events = [];
    this.waiters = [];
    socket.on("message", (message, rinfo) => {
      let packet;
      try {
        packet = decodeTransport(message);
      } catch {
        return;
      }
      const event = { packet, message, rinfo, frames: scanFrames(packet.payload) };
      this.events.push(event);
      for (let index = 0; index < this.waiters.length; index += 1) {
        const waiter = this.waiters[index];
        if (waiter.predicate(event)) {
          this.waiters.splice(index, 1);
          waiter.resolve(event);
          return;
        }
      }
    });
  }

  waitFor(predicate, timeoutMs = 1000) {
    const existing = this.events.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((item) => item.resolve !== resolve);
        reject(new Error("timed out waiting for UDP event"));
      }, timeoutMs);
      this.waiters.push({
        predicate,
        resolve: (event) => {
          clearTimeout(timer);
          resolve(event);
        },
      });
    });
  }

  waitForFrame(predicate, timeoutMs = 1000) {
    return this.waitFor((event) => event.frames.some((candidate) => predicate(candidate, event)), timeoutMs);
  }
}

async function startServer(extraArgs = []) {
  const options = parseArgs([
    "--udp-port", "0",
    "--tcp-port", "0",
    "--http-port", "0",
    "--json-log",
    ...extraArgs,
  ]);
  const server = new MockCameraServer(options);
  try {
    await server.listen();
  } catch (error) {
    server.stop();
    throw error;
  }
  return server;
}

async function sendUdp(socket, packet, port) {
  await new Promise((resolve, reject) => socket.send(packet, port, "127.0.0.1", (error) => error ? reject(error) : resolve()));
}

async function bindUdp() {
  const socket = dgram.createSocket("udp4");
  await new Promise((resolve, reject) => {
    socket.once("error", reject);
    socket.bind(0, "127.0.0.1", resolve);
  });
  return socket;
}

async function httpRequest(port, requestPath, headers = {}, method = "GET", body = null) {
  return new Promise((resolve, reject) => {
    const request = http.request({ host: "127.0.0.1", port, path: requestPath, method, headers }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks) }));
    });
    request.on("error", reject);
    request.end(body);
  });
}

function waitMs(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function replyFlags(command) {
  return command.cmdSet === 0x03 || command.cmdSet === 0x04 ? 0x80 : FLAG_RESPONSE;
}

test("DUML and transport codecs round-trip and recover stream fragmentation", () => {
  const original = frame({ cmdSet: 0x02, cmdId: 0x8e, payload: Buffer.from([0, 1, 0x38, 0]) });
  const encoded = encodeDuml(original);
  assert.equal(decodeDuml(encoded).status, "ok");
  assert.deepEqual(decodeDuml(encoded).frame, original);

  const decoder = new DumlStreamDecoder();
  assert.deepEqual(decoder.push(encoded.subarray(0, 4)), []);
  assert.deepEqual(decoder.push(Buffer.concat([encoded.subarray(4), encoded])), [original, original]);

  const datagram = wrapCommand(original, { sessionId: 7, transportSeq: 0x1008, cmdCounter: 3 });
  const packet = decodeTransport(datagram);
  assert.equal(packet.pktType, 0x05);
  assert.deepEqual(scanFrames(packet.payload), [original]);

  const badHeader = Buffer.from(datagram);
  badHeader[7] ^= 0xff;
  assert.throws(() => decodeTransport(badHeader), ProtocolError);
});

test("subscription and command validators use the captured wire lengths", () => {
  const name = "camcap_video_format";
  const subscription = subscriptionPayload(name, 0x69df);
  assert.deepEqual(parseSubscription(subscription), { name, subId: 0x69df });

  const state = new MockCameraState({ model: "pocket4pro" });
  assert.equal(state.validate(frame({ cmdSet: 0x00, cmdId: 0x99, payload: subscription })).ok, true);
  assert.equal(state.validate(frame({ cmdSet: 0x00, cmdId: 0x26, payload: listPayload(2, 0x40000001) })).ok, true);
  assert.equal(state.validate(frame({ cmdSet: 0x00, cmdId: 0x28, payload: Buffer.from([1, 1, 0, 0, 0x40, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0]) })).ok, true);
  assert.equal(state.validate(frame({ cmdSet: 0x00, cmdId: 0x28, payload: Buffer.alloc(17) })).ok, false);
  assert.equal(state.validate(frame({ cmdSet: 0x02, cmdId: 0xbf, payload: Buffer.concat([Buffer.from([1, 1]), u32(0x40000001), u32(1), Buffer.from([0, 1, 0, 0, 0])]) })).ok, true);
  assert.equal(state.validate(frame({ cmdSet: 0x09, cmdId: 0xa8, receiver: 0x01, payload: Buffer.alloc(10) })).ok, false);
  assert.equal(state.validate(frame({ cmdSet: 0x02, cmdId: 0x02, payload: Buffer.from([2]) })).ok, false);
});

test("public entrypoint exports the server and rejects malformed control patches", () => {
  assert.equal(publicApi.MockCameraServer, MockCameraServer);
  const state = new MockCameraState({ model: "pocket4pro" });
  publicApi.applyControlPatch(state, { batteryPercent: 42, focusPoint: { x: 0.25, y: 0.75 } });
  assert.equal(state.batteryPercent, 42);
  assert.equal(state.focusX, 0.25);
  assert.equal(state.focusY, 0.75);
  assert.throws(() => publicApi.applyControlPatch(state, { batteryPercent: "42" }), /batteryPercent must be an integer/);
  assert.throws(() => publicApi.applyControlPatch(state, { typo: true }), /unknown state field typo/);
  assert.throws(() => publicApi.applyControlPatch(state, { batteryPercent: 41, focusPoint: { x: 2, y: 0.5 } }), /value must be between 0 and 1/);
  assert.equal(state.batteryPercent, 42);
  assert.equal(state.focusX, 0.25);
  assert.throws(() => publicApi.applyControlPatch(state, { recording: true, inPlayback: true }), /cannot both be true/);
  assert.equal(state.recording, false);
  assert.equal(state.inPlayback, false);
  assert.throws(() => publicApi.applyControlPatch(state, { sdFreeMb: 200000 }), /sdFreeMb cannot exceed sdTotalMb/);
  assert.equal(state.sdFreeMb, 96000);
  assert.throws(() => publicApi.applyControlPatch(state, { vocalBoost: true }), /vocalBoost must be an integer/);
});

test("session 53/10 returns the captured four-byte wake reply", () => {
  const server = new MockCameraServer({ udpPort: 0, tcpPort: 0, httpPort: 0 });
  const reply = server.replyPayload(frame({ cmdSet: 0x53, cmdId: 0x10, payload: Buffer.alloc(4) }));
  assert.deepEqual(reply, Buffer.from([1, 0, 0, 0]));
  server.stop();
});

test("profiles enforce Nano live routing and camera state transitions", () => {
  const pocket = new MockCameraState({ model: "pocket4pro" });
  const nano = new MockCameraState({ model: "nano" });
  const livePayload = Buffer.from([0, 4, 2, 0, 0, 0, 0, 0, 0, 0]);
  assert.equal(nano.validate(frame({ cmdSet: 0x09, cmdId: 0xa8, receiver: 0x41, payload: livePayload })).ok, true);
  assert.equal(nano.validate(frame({ cmdSet: 0x09, cmdId: 0xa8, receiver: 0x08, payload: livePayload })).ok, false);
  assert.equal(nano.validate(frame({ cmdSet: 0x02, cmdId: 0x09, payload: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]) })).ok, true);
  assert.equal(pocket.validate(frame({ cmdSet: 0x02, cmdId: 0x09, payload: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]) })).ok, false);
  assert.equal(nano.statusFrames().some((candidate) => candidate.cmdSet === 0x04), false);

  const start = frame({ cmdSet: 0x02, cmdId: 0x02, payload: Buffer.from([1]) });
  assert.equal(pocket.validate(start).ok, true);
  pocket.apply(start);
  assert.equal(pocket.statusFrames().find((candidate) => candidate.cmdId === 0x80).payload.readUInt32LE(0) & 0x80, 0x80);
  assert.equal(pocket.validate(start).ok, false);
  const stop = frame({ cmdSet: 0x02, cmdId: 0x02, payload: Buffer.from([0]) });
  assert.equal(pocket.validate(stop).ok, true);
  pocket.apply(stop);
  assert.equal(pocket.validate(stop).ok, false);
  assert.equal(pocket.validate(frame({ cmdSet: 0x02, cmdId: 0x01, payload: Buffer.from([1]) })).ok, false);
  pocket.shootingMode = 0x17;
  assert.equal(pocket.validate(frame({ cmdSet: 0x02, cmdId: 0x01, payload: Buffer.from([1]) })).ok, true);
});

test("media manifest exposes parser-compatible handles and resolution", () => {
  const state = new MockCameraState({ model: "pocket4pro" });
  const bytes = mediaManifest(state.media);
  assert.equal(bytes.readUInt32LE(0), state.media.length);
  let marker = -1;
  for (let index = 0; index + 3 < bytes.length; index += 1) {
    if (bytes[index] === 0x03 && (bytes[index + 1] === 0xfe || bytes[index + 1] === 0xff) && bytes[index + 2] === 0x19 && bytes[index + 3] === 0x06) {
      marker = index;
      break;
    }
  }
  assert.ok(marker > 8);
  assert.equal(bytes.readUInt32LE(marker - 8), state.media[0].handle);
  assert.equal(bytes[marker - 8 + 7], 0x10);
  assert.equal(bytes[marker + 10], 1);
});

test("server completes UDP handshake, subscription, media, TCP pairing, and HTTP Range", async (t) => {
  const server = await startServer();
  let udp;
  let tcp;
  t.after(async () => {
    if (udp) udp.close();
    if (tcp) tcp.destroy();
    server.stop();
    await new Promise((resolve) => setTimeout(resolve, 20));
  });

  udp = dgram.createSocket("udp4");
  await new Promise((resolve, reject) => {
    udp.once("error", reject);
    udp.bind(0, "127.0.0.1", resolve);
  });
  const collector = new UdpCollector(udp);
  const serverUdpPort = server.udp.address().port;
  const sessionId = 0x3344;
  const baseSeq = 0x1200;
  await sendUdp(udp, handshakeDatagram({ sessionId, seq: baseSeq, baseSeq }), serverUdpPort);
  const handshake = await collector.waitFor((event) => event.packet.pktType === PKT_HANDSHAKE);
  assert.equal(handshake.packet.sessionId, sessionId);
  assert.equal(handshake.packet.payload.readUInt16LE(0), baseSeq);
  const telemetry = await collector.waitFor((event) => event.packet.pktType === PKT_TELEMETRY);
  assert.equal(telemetry.message.length, 34);

  let transportSeq = baseSeq + 8;
  let commandCounter = 1;
  let frameSeq = 0x100;
  const sendCommand = async (command) => {
    const packet = wrapCommand(command, { sessionId, transportSeq, cmdCounter: commandCounter });
    await sendUdp(udp, packet, serverUdpPort);
    transportSeq = (transportSeq + 8) & 0xffff;
    commandCounter = (commandCounter + 1) & 0xff;
  };
  const waitReply = (command) => collector.waitForFrame((candidate) => candidate.seq === command.seq && candidate.cmdSet === command.cmdSet && candidate.cmdId === command.cmdId && candidate.flags === (command.cmdSet === 0x03 || command.cmdSet === 0x04 ? 0x80 : FLAG_RESPONSE));

  const register = frame({ receiver: 0x48, seq: 0x100, flags: 0x80, cmdSet: 0x00, cmdId: 0x81, payload: registerPayload() });
  const registerReply = waitReply(register);
  await sendCommand(register);
  assert.deepEqual((await registerReply).frames.find((candidate) => candidate.seq === register.seq).payload, Buffer.from([0]));

  const presence = frame({ receiver: 0x28, seq: ++frameSeq, cmdSet: 0x00, cmdId: 0x88, payload: presencePayload() });
  const presenceReply = waitReply(presence);
  await sendCommand(presence);
  assert.deepEqual((await presenceReply).frames.find((candidate) => candidate.seq === presence.seq).payload, Buffer.from([0]));

  const gimbal = frame({ receiver: 0x03, seq: ++frameSeq, cmdSet: 0x03, cmdId: 0xda, payload: Buffer.from([5, 0xff, 0xff, 0xff, 0xff]) });
  const gimbalReply = waitReply(gimbal);
  await sendCommand(gimbal);
  assert.deepEqual((await gimbalReply).frames.find((candidate) => candidate.seq === gimbal.seq).payload, Buffer.from([0]));

  const subscription = frame({ receiver: 0x28, seq: ++frameSeq, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("camcap_video_format", 0x69df) });
  const subscriptionReply = waitReply(subscription);
  await sendCommand(subscription);
  const subscriptionEvent = await subscriptionReply;
  assert.deepEqual(subscriptionEvent.frames.find((candidate) => candidate.seq === subscription.seq).payload, Buffer.from([0]));
  await collector.waitForFrame((candidate) => candidate.cmdSet === 0x00 && candidate.cmdId === 0x99 && candidate.flags === 0 && candidate.payload[0] === 0x02 && candidate.payload[1] === 0x06);

  const scan = frame({ receiver: 0x07, seq: ++frameSeq, cmdSet: 0x07, cmdId: 0xab, payload: Buffer.alloc(0) });
  const scanReply = waitReply(scan);
  await sendCommand(scan);
  assert.deepEqual((await scanReply).frames.find((candidate) => candidate.seq === scan.seq).payload, Buffer.from([0]));
  const scanReport = await collector.waitForFrame((candidate) => candidate.cmdSet === 0x07 && candidate.cmdId === 0xac && candidate.sender === 0x07);
  assert.deepEqual(scanReport.frames.find((candidate) => candidate.cmdId === 0xac).payload.subarray(0, 4), Buffer.from([1, 0x11, 0, 0]));

  const role = frame({ receiver: 0x07, seq: ++frameSeq, cmdSet: 0x07, cmdId: 0x39, payload: Buffer.from([0]) });
  const roleReply = waitReply(role);
  await sendCommand(role);
  assert.deepEqual((await roleReply).frames.find((candidate) => candidate.seq === role.seq).payload, Buffer.from([0, 0]));
  const station = frame({ receiver: 0x07, seq: ++frameSeq, cmdSet: 0x07, cmdId: 0x48, payload: Buffer.from([1]) });
  const stationReply = waitReply(station);
  await sendCommand(station);
  assert.deepEqual((await stationReply).frames.find((candidate) => candidate.seq === station.seq).payload, Buffer.from([0, 0]));
  const roleAfter = frame({ receiver: 0x07, seq: ++frameSeq, cmdSet: 0x07, cmdId: 0x39, payload: Buffer.from([0]) });
  const roleAfterReply = waitReply(roleAfter);
  await sendCommand(roleAfter);
  assert.deepEqual((await roleAfterReply).frames.find((candidate) => candidate.seq === roleAfter.seq).payload, Buffer.from([0, 1]));

  const prepare = frame({ receiver: 0x01, seq: ++frameSeq, cmdSet: 0x02, cmdId: 0x68, payload: Buffer.from([8]) });
  const prepareReply = waitReply(prepare);
  await sendCommand(prepare);
  assert.deepEqual((await prepareReply).frames.find((candidate) => candidate.seq === prepare.seq).payload, Buffer.from([0]));
  const live = frame({ receiver: 0x08, seq: ++frameSeq, cmdSet: 0x09, cmdId: 0xa8, payload: Buffer.from([0, 4, 2, 0, 0, 0, 0, 0, 0, 0]) });
  const liveReply = waitReply(live);
  await sendCommand(live);
  assert.deepEqual((await liveReply).frames.find((candidate) => candidate.seq === live.seq).payload, Buffer.from([0]));

  const mediaSd = frame({ receiver: 0x01, seq: ++frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: listPayload(1, 0x40000001) });
  const mediaSdReply = waitReply(mediaSd);
  await sendCommand(mediaSd);
  assert.deepEqual((await mediaSdReply).frames.find((candidate) => candidate.seq === mediaSd.seq).payload, Buffer.from([0]));
  const mediaTrigger = frame({ receiver: 0x01, seq: ++frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: Buffer.from([0x4a, 0x04, 0x0e, 0x10, 0x01, 0, 0, 0, 0, 0, 1, 0, 0, 0]) });
  const mediaTriggerReply = waitReply(mediaTrigger);
  await sendCommand(mediaTrigger);
  assert.deepEqual((await mediaTriggerReply).frames.find((candidate) => candidate.seq === mediaTrigger.seq).payload, Buffer.from([0]));
  const mediaRequest = frame({ receiver: 0x01, seq: ++frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: listPayload(2, 0x40000001) });
  const mediaReply = waitReply(mediaRequest);
  const mediaStart = collector.events.length;
  await sendCommand(mediaRequest);
  assert.deepEqual((await mediaReply).frames.find((candidate) => candidate.seq === mediaRequest.seq).payload, Buffer.from([0]));
  await collector.waitForFrame((candidate) => candidate.cmdSet === 0x00 && candidate.cmdId === 0x27 && candidate.payload[1] === 0x03 && candidate.payload[4] === 2);
  const chunks = collector.events.slice(mediaStart).flatMap((event) => event.frames.filter((candidate) => candidate.cmdSet === 0x00 && candidate.cmdId === 0x27 && candidate.payload[1] === 0x01 && candidate.payload[4] === 2).map((candidate) => candidate.payload.subarray(10)));
  const manifest = Buffer.concat(chunks);
  assert.equal(manifest.readUInt32LE(0), 3);

  tcp = net.createConnection({ host: "127.0.0.1", port: server.tcp.address().port });
  await once(tcp, "connect");
  const tcpDecoder = new DumlStreamDecoder();
  const tcpReply = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for TCP pairing reply")), 1000);
    tcp.on("data", (chunk) => {
      const frames = tcpDecoder.push(chunk);
      if (frames.length) {
        clearTimeout(timer);
        resolve(frames[0]);
      }
    });
    tcp.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
  const pairing = encodeDuml(frame({ receiver: 0x07, seq: 0x8092, cmdSet: 0x07, cmdId: 0x45, payload: pairingPayload() }));
  tcp.write(pairing.subarray(0, 7));
  tcp.write(pairing.subarray(7));
  assert.deepEqual((await tcpReply).payload, Buffer.from([0, 1]));
  const tcpResponses = server.metrics.responses;
  server.options.dropAcks = true;
  tcp.write(pairing);
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.equal(server.metrics.responses, tcpResponses);
  server.options.dropAcks = false;

  const health = await httpRequest(server.http.address().port, "/health");
  assert.equal(health.status, 200);
  assert.equal(JSON.parse(health.body).ok, true);
  const mediaPath = encodeURIComponent(server.state.media[0].path);
  const virtualMedia = Buffer.from(`OpenPocketCine mock media: ${server.state.media[0].path}\n`, "ascii");
  const ranged = await httpRequest(server.http.address().port, `/v2?storage=1&path=${mediaPath}`, { Range: "bytes=1-3" });
  assert.equal(ranged.status, 206);
  assert.equal(ranged.body.length, 3);
  assert.equal(ranged.headers["content-range"], `bytes 1-3/${virtualMedia.length}`);
  const suffix = await httpRequest(server.http.address().port, `/v2?storage=1&path=${mediaPath}`, { Range: "bytes=-4" });
  assert.equal(suffix.status, 206);
  assert.equal(suffix.body.length, 4);
  const traversal = await httpRequest(server.http.address().port, "/v2?storage=1&path=..%2Fsecret");
  assert.equal(traversal.status, 400);
  const wrongStorage = await httpRequest(server.http.address().port, `/v2?storage=0&path=${mediaPath}`);
  assert.equal(wrongStorage.status, 404);

  const controlPort = server.http.address().port;
  const fault = await httpRequest(controlPort, "/control", { "Content-Type": "application/json" }, "POST", JSON.stringify({ action: "fault", name: "dropVideo", value: true }));
  assert.equal(fault.status, 200);
  assert.equal(server.options.dropVideo, true);
  const badControl = await httpRequest(controlPort, "/control", { "Content-Type": "application/json" }, "POST", JSON.stringify({ action: "fault", name: "unknown", value: true }));
  assert.equal(badControl.status, 400);

  const mediaRoot = await mkdtemp(path.join(os.tmpdir(), "opc-mock-media-"));
  try {
    await mkdir(path.join(mediaRoot, path.dirname(server.state.media[0].path)), { recursive: true });
    const source = Buffer.from("real mock-root bytes", "ascii");
    await writeFile(path.join(mediaRoot, server.state.media[0].path), source);
    server.options.mediaRoot = mediaRoot;
    const mapped = await httpRequest(controlPort, `/v2?storage=1&path=${mediaPath}`);
    assert.deepEqual(mapped.body, source);
  } finally {
    await rm(mediaRoot, { recursive: true, force: true });
  }
});

test("UDP rebind keeps a handshaken session when the client changes its ephemeral port", async (t) => {
  const server = await startServer();
  let first;
  let rebound;
  t.after(async () => {
    first?.close();
    rebound?.close();
    server.stop();
    await new Promise((resolve) => setTimeout(resolve, 20));
  });

  const bind = async () => {
    const socket = dgram.createSocket("udp4");
    await new Promise((resolve, reject) => {
      socket.once("error", reject);
      socket.bind(0, "127.0.0.1", resolve);
    });
    return socket;
  };
  first = await bind();
  const firstCollector = new UdpCollector(first);
  const serverPort = server.udp.address().port;
  const sessionId = 0x4455;
  const baseSeq = 0x2200;
  await sendUdp(first, handshakeDatagram({ sessionId, seq: baseSeq, baseSeq }), serverPort);
  await firstCollector.waitFor((event) => event.packet.pktType === PKT_HANDSHAKE);

  rebound = await bind();
  const reboundCollector = new UdpCollector(rebound);
  await sendUdp(rebound, ackDatagram({ sessionId, video: baseSeq, ackedData: baseSeq, extra: baseSeq }), serverPort);
  const command = frame({ receiver: 0x07, seq: 0x2201, cmdSet: 0x07, cmdId: 0x07, payload: Buffer.alloc(0) });
  const commandReply = reboundCollector.waitForFrame((candidate) => candidate.seq === command.seq && candidate.flags === FLAG_RESPONSE);
  await sendUdp(rebound, wrapCommand(command, { sessionId, transportSeq: baseSeq + 8, cmdCounter: 1 }), serverPort);

  assert.deepEqual((await commandReply).frames[0].payload, Buffer.concat([Buffer.from([0]), Buffer.from([server.state.ssid.length]), Buffer.from(server.state.ssid)]));
  assert.equal(server.metrics.invalidPackets, 0);
  assert.equal(server.sessions.size, 1);
  assert.equal([...server.sessions.values()][0].port, rebound.address().port);
});

test("strict UDP mode drops bad sequencing and rejects invalid state transitions", async (t) => {
  const server = await startServer();
  const udp = await bindUdp();
  t.after(async () => {
    udp.close();
    server.stop();
    await waitMs(20);
  });

  const collector = new UdpCollector(udp);
  const serverPort = server.udp.address().port;
  const sessionId = 0x5566;
  const baseSeq = 0x3000;
  let transportSeq = baseSeq + 8;
  let commandCounter = 1;
  let frameSeq = 0x300;

  const sendWire = async (command, {
    transportSeqValue = transportSeq,
    counterValue = commandCounter,
    mutatePacket = null,
    advance = true,
    expectReply = true,
  } = {}) => {
    const waiter = expectReply
      ? collector.waitForFrame((candidate) => candidate.seq === command.seq && candidate.cmdSet === command.cmdSet && candidate.cmdId === command.cmdId && candidate.flags === replyFlags(command))
      : null;
    let packet = wrapCommand(command, { sessionId, transportSeq: transportSeqValue, cmdCounter: counterValue });
    if (mutatePacket) packet = mutatePacket(packet);
    await sendUdp(udp, packet, serverPort);
    if (advance) {
      transportSeq = (transportSeq + 8) & 0xffff;
      commandCounter = (commandCounter + 1) & 0xff;
    }
    if (!waiter) {
      await waitMs(20);
      return null;
    }
    return (await waiter).frames.find((candidate) => candidate.seq === command.seq && candidate.cmdSet === command.cmdSet && candidate.cmdId === command.cmdId).payload;
  };

  const unhandshaken = frame({ receiver: 0x07, seq: frameSeq, cmdSet: 0x07, cmdId: 0x07, payload: Buffer.alloc(0) });
  await sendUdp(udp, wrapCommand(unhandshaken, { sessionId, transportSeq, cmdCounter: commandCounter }), serverPort);
  await waitMs(20);
  assert.equal(server.metrics.droppedPackets, 1);

  await sendUdp(udp, handshakeDatagram({ sessionId, seq: baseSeq, baseSeq }), serverPort);
  await collector.waitFor((event) => event.packet.pktType === PKT_HANDSHAKE);
  await collector.waitFor((event) => event.packet.pktType === PKT_TELEMETRY);

  const register = frame({ receiver: 0x48, seq: frameSeq, flags: 0x80, cmdSet: 0x00, cmdId: 0x81, payload: registerPayload() });
  await sendWire(register, { transportSeqValue: transportSeq + 8, expectReply: false, advance: false });
  assert.equal(server.metrics.droppedPackets, 2);
  assert.deepEqual(await sendWire(register), Buffer.from([0]));
  frameSeq += 1;

  const presence = frame({ receiver: 0x28, seq: frameSeq, cmdSet: 0x00, cmdId: 0x88, payload: presencePayload() });
  await sendWire(presence, { counterValue: 0x7f, expectReply: false, advance: false });
  assert.equal(server.metrics.droppedPackets, 3);
  assert.deepEqual(await sendWire(presence), Buffer.from([0]));
  frameSeq += 1;

  const subscriptionBeforeGimbal = frame({ receiver: 0x28, seq: frameSeq, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("camcap_video_format", 0x69df) });
  assert.deepEqual(await sendWire(subscriptionBeforeGimbal), Buffer.from([0xd9]));
  frameSeq += 1;

  const gimbal = frame({ receiver: 0x03, seq: frameSeq + 1, cmdSet: 0x03, cmdId: 0xda, payload: Buffer.from([5, 0xff, 0xff, 0xff, 0xff]) });
  await sendWire(gimbal, { expectReply: false, advance: false });
  assert.equal(server.metrics.droppedPackets, 4);
  gimbal.seq = frameSeq;
  assert.deepEqual(await sendWire(gimbal), Buffer.from([0]));
  frameSeq += 1;

  const subscription = frame({ receiver: 0x28, seq: frameSeq, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("camcap_video_format", 0x69df) });
  assert.deepEqual(await sendWire(subscription, { mutatePacket: (packet) => { const corrupted = Buffer.from(packet); corrupted[12] = 1; return corrupted; }, expectReply: false, advance: false }), null);
  assert.equal(server.metrics.droppedPackets, 5);
  assert.deepEqual(await sendWire(subscription), Buffer.from([0]));
  frameSeq += 1;

  const duplicateSubscription = frame({ receiver: 0x28, seq: frameSeq, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("camcap_video_format", 0x69e0) });
  assert.deepEqual(await sendWire(duplicateSubscription), Buffer.from([0xd9]));
  frameSeq += 1;
  const unknownSubscription = frame({ receiver: 0x28, seq: frameSeq, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("unsupported_status", 0x69e0) });
  assert.deepEqual(await sendWire(unknownSubscription), Buffer.from([0xe0]));
  frameSeq += 1;

  const internalFirst = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: listPayload(2, 0x40000001) });
  assert.deepEqual(await sendWire(internalFirst), Buffer.from([0xd9]));
  frameSeq += 1;
  const sdList = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: listPayload(1, 0x40000001) });
  assert.deepEqual(await sendWire(sdList), Buffer.from([0]));
  frameSeq += 1;
  const mediaTrigger = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: Buffer.from([0x4a, 0x04, 0x0e, 0x10, 0x01, 0, 0, 0, 0, 0, 1, 0, 0, 0]) });
  assert.deepEqual(await sendWire(mediaTrigger), Buffer.from([0]));
  frameSeq += 1;
  const internalList = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x00, cmdId: 0x26, payload: listPayload(2, 0x40000001) });
  assert.deepEqual(await sendWire(internalList), Buffer.from([0]));
  frameSeq += 1;

  const missingHandle = Buffer.alloc(18);
  missingHandle[0] = 1;
  missingHandle.writeUInt32LE(0xdeadbeef, 1);
  missingHandle[10] = 1;
  missingHandle[11] = 1;
  const deleteMissing = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x00, cmdId: 0x28, payload: missingHandle });
  assert.deepEqual(await sendWire(deleteMissing), Buffer.from([0xd9]));
  assert.equal(server.state.media.length, 3);
  frameSeq += 1;

  const enterPlayback = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x02, cmdId: 0x0c, payload: Buffer.from([1, 1, 0, 1]) });
  assert.deepEqual(await sendWire(enterPlayback), Buffer.from([0]));
  frameSeq += 1;
  const recordInPlayback = frame({ receiver: 0x01, seq: frameSeq, cmdSet: 0x02, cmdId: 0x02, payload: Buffer.from([1]) });
  assert.deepEqual(await sendWire(recordInPlayback), Buffer.from([0xd9]));
  assert.equal(server.state.recording, false);
  frameSeq += 1;
  const liveInPlayback = frame({ receiver: 0x08, seq: frameSeq, cmdSet: 0x09, cmdId: 0xa8, payload: Buffer.from([0, 4, 2, 0, 0, 0, 0, 0, 0, 0]) });
  assert.deepEqual(await sendWire(liveInPlayback), Buffer.from([0xd9]));
  assert.equal(server.state.liveEnabled, false);

  assert.ok(server.metrics.outOfOrderPackets >= 4);
  assert.ok(server.metrics.stateRejected >= 5);
  assert.ok(server.metrics.unsupportedCommands >= 1);
});

test("strict UDP mode expires presence leases", async (t) => {
  const server = await startServer(["--presence-timeout-ms", "20"]);
  const udp = await bindUdp();
  t.after(async () => {
    udp.close();
    server.stop();
    await waitMs(20);
  });

  const collector = new UdpCollector(udp);
  const serverPort = server.udp.address().port;
  const sessionId = 0x6677;
  const baseSeq = 0x3800;
  let transportSeq = baseSeq + 8;
  let commandCounter = 1;
  let frameSeq = 0x400;
  const issue = async (command) => {
    const waiter = collector.waitForFrame((candidate) => candidate.seq === command.seq && candidate.cmdSet === command.cmdSet && candidate.cmdId === command.cmdId && candidate.flags === replyFlags(command));
    await sendUdp(udp, wrapCommand(command, { sessionId, transportSeq, cmdCounter: commandCounter }), serverPort);
    transportSeq = (transportSeq + 8) & 0xffff;
    commandCounter = (commandCounter + 1) & 0xff;
    return (await waiter).frames.find((candidate) => candidate.seq === command.seq && candidate.cmdSet === command.cmdSet && candidate.cmdId === command.cmdId).payload;
  };

  await sendUdp(udp, handshakeDatagram({ sessionId, seq: baseSeq, baseSeq }), serverPort);
  await collector.waitFor((event) => event.packet.pktType === PKT_HANDSHAKE);
  await collector.waitFor((event) => event.packet.pktType === PKT_TELEMETRY);

  await sendUdp(udp, ackDatagram({ sessionId, video: baseSeq, ackedData: baseSeq, extra: baseSeq }), serverPort);
  await waitMs(10);
  const outOfOrderBeforeStaleAck = server.metrics.outOfOrderPackets;
  await sendUdp(udp, ackDatagram({ sessionId, video: baseSeq - 1, ackedData: baseSeq - 1, extra: baseSeq - 1 }), serverPort);
  await waitMs(10);
  assert.equal(server.metrics.staleAcks, 3);
  assert.equal(server.metrics.outOfOrderPackets, outOfOrderBeforeStaleAck);
  assert.equal([...server.sessions.values()][0].videoSeq, baseSeq);
  assert.equal([...server.sessions.values()][0].ackedData, baseSeq);

  const malformedAck = ackDatagram({ sessionId, video: baseSeq, ackedData: baseSeq, extra: baseSeq });
  malformedAck[8 + 4] = 1;
  await sendUdp(udp, malformedAck, serverPort);
  await waitMs(10);
  assert.equal(server.metrics.staleAcks, 3);
  assert.equal(server.metrics.malformedPackets, 1);

  assert.deepEqual(await issue(frame({ receiver: 0x48, seq: frameSeq++, flags: 0x80, cmdSet: 0x00, cmdId: 0x81, payload: registerPayload() })), Buffer.from([0]));
  assert.deepEqual(await issue(frame({ receiver: 0x28, seq: frameSeq++, cmdSet: 0x00, cmdId: 0x88, payload: presencePayload() })), Buffer.from([0]));
  assert.deepEqual(await issue(frame({ receiver: 0x03, seq: frameSeq++, cmdSet: 0x03, cmdId: 0xda, payload: Buffer.from([5, 0xff, 0xff, 0xff, 0xff]) })), Buffer.from([0]));
  assert.deepEqual(await issue(frame({ receiver: 0x28, seq: frameSeq++, cmdSet: 0x00, cmdId: 0x99, payload: subscriptionPayload("camcap_video_format", 0x69df) })), Buffer.from([0]));
  await waitMs(35);
  const expired = frame({ receiver: 0x01, seq: frameSeq++, cmdSet: 0x02, cmdId: 0x02, payload: Buffer.from([1]) });
  assert.deepEqual(await issue(expired), Buffer.from([0xd9]));
  assert.equal(server.state.recording, false);
  assert.equal([...server.sessions.values()][0].presence, false);
});

test("TCP requires pairing first and keeps duplicate pairing idempotent", async (t) => {
  const server = await startServer();
  const socket = net.createConnection({ host: "127.0.0.1", port: server.tcp.address().port });
  const decoder = new DumlStreamDecoder();
  t.after(async () => {
    socket.destroy();
    server.stop();
    await waitMs(20);
  });
  await once(socket, "connect");

  const waitTcpFrame = () => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for TCP response")), 1000);
    const onData = (chunk) => {
      const frames = decoder.push(chunk);
      if (!frames.length) return;
      clearTimeout(timer);
      socket.off("data", onData);
      resolve(frames[0]);
    };
    socket.on("data", onData);
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });

  const notPairing = encodeDuml(frame({ receiver: 0x07, seq: 1, cmdSet: 0x07, cmdId: 0x07, payload: Buffer.alloc(0) }));
  const firstReply = waitTcpFrame();
  socket.write(notPairing);
  assert.deepEqual((await firstReply).payload, Buffer.from([0xe0]));
  assert.equal(server.metrics.acceptedCommands, 0);

  const pairing = encodeDuml(frame({ receiver: 0x07, seq: 7, cmdSet: 0x07, cmdId: 0x45, payload: pairingPayload() }));
  const pairReply = waitTcpFrame();
  socket.write(pairing);
  assert.deepEqual((await pairReply).payload, Buffer.from([0, 1]));
  assert.equal(server.metrics.acceptedCommands, 1);

  const changedPairing = encodeDuml(frame({ receiver: 0x07, seq: 8, cmdSet: 0x07, cmdId: 0x45, payload: Buffer.concat([Buffer.from([3]), Buffer.from("abc"), Buffer.from([1, 0x78])]) }));
  const changedReply = waitTcpFrame();
  socket.write(changedPairing);
  assert.deepEqual((await changedReply).payload, Buffer.from([0xd9]));
  assert.equal(server.metrics.acceptedCommands, 1);
  assert.equal(server.metrics.stateRejected, 1);

  const acceptedBeforeDuplicate = server.metrics.acceptedCommands;
  const duplicateReply = waitTcpFrame();
  socket.write(pairing);
  assert.deepEqual((await duplicateReply).payload, Buffer.from([0, 1]));
  assert.equal(server.metrics.acceptedCommands, acceptedBeforeDuplicate);
  assert.ok(server.metrics.duplicatePackets >= 1);
});
