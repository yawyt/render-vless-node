'use strict';
const http = require('http');
const net = require('net');
const dgram = require('dgram');
const { WebSocketServer } = require('ws');
const PORT = parseInt(process.env.PORT || '3000', 10);
const UUID = (process.env.UUID || '').trim().toLowerCase();
const UUID_HEX = UUID.replace(/-/g, '');
let WSPATH = (process.env.WSPATH || '/9106f955f584').trim();
if (!WSPATH.startsWith('/')) WSPATH = '/' + WSPATH;
console.log('VLESS-WS server starting, port=' + PORT + ' path=' + WSPATH);
function parseAddr(buf, off) {
  const type = buf[off];
  if (type === 1) {
    if (off + 5 > buf.length) return null;
    const host = buf[off + 1] + '.' + buf[off + 2] + '.' + buf[off + 3] + '.' + buf[off + 4];
    return { host, next: off + 5 };
  }
  if (type === 2) {
    const len = buf[off + 1];
    if (off + 2 + len > buf.length) return null;
    const host = buf.toString('utf8', off + 2, off + 2 + len);
    return { host, next: off + 2 + len };
  }
  if (type === 3) {
    if (off + 17 > buf.length) return null;
    const parts = [];
    for (let i = 0; i < 16; i += 2) parts.push(buf.readUInt16BE(off + 1 + i).toString(16));
    return { host: parts.join(':'), next: off + 17 };
  }
  return null;
}
function addrBuf(host, port) {
  if (net.isIPv4(host)) {
    const b = Buffer.alloc(1 + 4 + 2);
    b[0] = 1;
    host.split('.').forEach((v, i) => { b[1 + i] = parseInt(v, 10); });
    b.writeUInt16BE(port, 5);
    return b;
  }
  if (net.isIPv6(host)) {
    return Buffer.concat([Buffer.from([3]), Buffer.from(host.split(':').flatMap(() => [])), Buffer.alloc(0)]);
  }
  const hb = Buffer.from(host, 'utf8');
  const b = Buffer.alloc(1 + 1 + hb.length + 2);
  b[0] = 2; b[1] = hb.length; hb.copy(b, 2); b.writeUInt16BE(port, 2 + hb.length);
  return b;
}
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('OK\n'); return; }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not Found\n');
});
const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  let path = req.url || '';
  try { path = new URL(req.url, 'http://x').pathname; } catch (e) {}
  if (path !== WSPATH) { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => { wss.emit('connection', ws, req); });
});
wss.on('connection', (ws) => {
  let remote = null;
  let udpSocket = null;
  let udpDest = null;
  let headerDone = false;
  let command = 0;
  let version = 0;
  const respHeader = () => Buffer.from([version, 0]);
  const cleanup = () => { try { if (remote) remote.destroy(); } catch (e) {} try { if (udpSocket) udpSocket.close(); } catch (e) {} };
  ws.on('close', cleanup);
  ws.on('error', cleanup);
  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!headerDone) {
      try {
        if (buf.length < 18) throw new Error('short header');
        version = buf[0];
        if (UUID_HEX && buf.slice(1, 17).toString('hex') !== UUID_HEX) { ws.close(); return; }
        const addonLen = buf[17];
        let off = 18 + addonLen;
        if (off + 4 > buf.length) throw new Error('short cmd');
        command = buf[off];
        const port = buf.readUInt16BE(off + 1);
        const addr = parseAddr(buf, off + 3);
        if (!addr) throw new Error('bad addr');
        const host = addr.host;
        const payload = buf.slice(addr.next);
        headerDone = true;
        if (command === 1) {
          remote = net.connect(port, host, () => {
            if (ws.readyState === 1) ws.send(respHeader());
            if (payload.length) remote.write(payload);
          });
          remote.on('data', (chunk) => { if (ws.readyState === 1) ws.send(chunk); });
          remote.on('error', () => { try { ws.close(); } catch (e) {} });
          remote.on('close', () => { try { ws.close(); } catch (e) {} });
        } else if (command === 2) {
          udpDest = { host, port };
          udpSocket = dgram.createSocket('udp4');
          udpSocket.on('message', (msg, rinfo) => {
            const dest = (rinfo && rinfo.address) ? { host: udpDest.host, port: udpDest.port } : udpDest;
            const ab = addrBuf(dest.host, dest.port);
            const pkt = Buffer.alloc(2 + ab.length + msg.length);
            pkt.writeUInt16BE(ab.length + msg.length, 0);
            ab.copy(pkt, 2); msg.copy(pkt, 2 + ab.length);
            if (ws.readyState === 1) ws.send(Buffer.concat([respHeader(), pkt]));
          });
          udpSocket.on('error', () => {});
          if (payload.length) udpSocket.send(payload, port, host, () => {});
        } else { ws.close(); }
      } catch (e) { try { ws.close(); } catch (err) {} }
      return;
    }
    if (command === 1) {
      if (remote && !remote.destroyed) remote.write(buf);
    } else if (command === 2) {
      if (!udpSocket) return;
      let off = 0;
      let handled = false;
      while (off + 2 <= buf.length) {
        const len = buf.readUInt16BE(off);
        if (len <= 0 || off + 2 + len > buf.length) break;
        const pkt = buf.slice(off + 2, off + 2 + len);
        const addr = parseAddr(pkt, 0);
        if (!addr || addr.next > pkt.length) break;
        const data = pkt.slice(addr.next);
        udpDest = { host: addr.host, port: pkt.readUInt16BE(addr.next - 2) };
        // port is before addr? VLESS UDP packet: addr then data, port is in header? Use parsed: port is last 2 bytes before data is not standard; fallback to header port
        udpSocket.send(data, udpDest.port || 53, addr.host, () => {});
        handled = true;
        off += 2 + len;
      }
      if (!handled && udpDest) udpSocket.send(buf, udpDest.port, udpDest.host, () => {});
      if (handled && off < buf.length && udpDest) udpSocket.send(buf.slice(off), udpDest.port, udpDest.host, () => {});
    }
  });
});
server.listen(PORT, '0.0.0.0', () => { console.log('Listening on ' + PORT); });
