const WritableAsyncIterableStream = require('../index');
const assert = require('assert');

let pendingTimeoutSet = new Set();

function wait(duration) {
  return new Promise((resolve) => {
    let timeout = setTimeout(() => {
      pendingTimeoutSet.clear(timeout);
      resolve();
    }, duration);
    pendingTimeoutSet.add(timeout);
  });
}

function cancelAllPendingWaits() {
  for (let timeout of pendingTimeoutSet) {
    clearTimeout(timeout);
  }
}

describe('WritableAsyncIterableStream', () => {
  let stream;

  beforeEach(async () => {
    stream = new WritableAsyncIterableStream();
  });

  afterEach(async () => {
    cancelAllPendingWaits();
    stream.end();
  });

  it('should receive packets asynchronously', async () => {
    (async () => {
      for (let i = 0; i < 10; i++) {
        await wait(10);
        stream.write('hello' + i);
      }
      stream.end();
    })();

    let receivedPackets = [];
    for await (let packet of stream) {
      receivedPackets.push(packet);
    }
    assert.equal(receivedPackets.length, 10);
  });

  it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
    (async () => {
      for (let i = 0; i < 10; i++) {
        await wait(10);
        stream.write('a' + i);
        stream.write('b' + i);
        stream.write('c' + i);
      }
      stream.end();
    })();

    let receivedPackets = [];
    for await (let packet of stream) {
      receivedPackets.push(packet);
    }
    assert.equal(receivedPackets.length, 30);
    assert.equal(receivedPackets[0], 'a0');
    assert.equal(receivedPackets[1], 'b0');
    assert.equal(receivedPackets[2], 'c0');
    assert.equal(receivedPackets[3], 'a1');
    assert.equal(receivedPackets[4], 'b1');
    assert.equal(receivedPackets[5], 'c1');
    assert.equal(receivedPackets[29], 'c9');
  });

  it('should receive packets if stream is written to from inside a consuming for-await-of loop', async () => {
    (async () => {
      for (let i = 0; i < 3; i++) {
        await wait(10);
        stream.write('a' + i);
      }
    })();

    let count = 0;
    let receivedPackets = [];
    for await (let packet of stream) {
      receivedPackets.push(packet);
      stream.write('nested' + count);
      if (++count > 10) {
        break;
      }
    }
    assert.equal(receivedPackets.length, 11);
    assert.equal(receivedPackets[0], 'a0');
    assert.equal(receivedPackets[1], 'nested0');
    assert.equal(receivedPackets[2], 'nested1');
    assert.equal(receivedPackets[10], 'nested9');
  });

  it('should receive next packet asynchronously when once() method is used', async () => {
    (async () => {
      for (let i = 0; i < 3; i++) {
        await wait(10);
        stream.write('a' + i);
      }
    })();

    let nextPacket = await stream.once();
    assert.equal(nextPacket, 'a0');

    nextPacket = await stream.once();
    assert.equal(nextPacket, 'a1');

    nextPacket = await stream.once();
    assert.equal(nextPacket, 'a2');
  });
});
