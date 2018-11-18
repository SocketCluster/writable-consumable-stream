const IterableAsyncStream = require('../index');
const assert = require('assert');

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, duration);
  });
}

describe('IterableAsyncStream', () => {
  let stream;

  beforeEach(async () => {
    stream = new IterableAsyncStream();
  });

  afterEach(async () => {
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

  it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
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
});
