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

  describe('for-await-of loop', () => {
    beforeEach(async () => {
      stream = new WritableAsyncIterableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should receive packets asynchronously', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      for await (let packet of stream) {
        receivedPackets.push(packet);
      }
      assert.equal(receivedPackets.length, 10);
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('a' + i);
          stream.write('b' + i);
          stream.write('c' + i);
        }
        stream.close();
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
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
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
      assert.equal(receivedPackets[0], 'a0');
      assert.equal(receivedPackets.some(message => message === 'nested0'), true);
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should only consume messages which were written after the consumer was created', async () => {
      stream.write('one');
      stream.write('two');

      let receivedPackets = [];

      let doneConsumingPromise = (async () => {
        for await (let packet of stream) {
          receivedPackets.push(packet);
        }
      })();

      stream.write('three');
      stream.write('four');
      stream.write('five');
      stream.close();

      await doneConsumingPromise;

      assert.equal(receivedPackets.length, 3);
      assert.equal(receivedPackets[0], 'three');
      assert.equal(receivedPackets[1], 'four');
      assert.equal(receivedPackets[2], 'five');
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should not miss packets if it awaits inside a for-await-of loop', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(2);
          stream.write('a' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      for await (let packet of stream) {
        receivedPackets.push(packet);
        await wait(50);
      }

      assert.equal(receivedPackets.length, 10);
      for (let i = 0; i < 10; i++) {
        assert.equal(receivedPackets[i], 'a' + i);
      }
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should not miss packets if it awaits inside two concurrent for-await-of loops', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('a' + i);
        }
        stream.close();
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.all([
        (async () => {
          for await (let packet of stream) {
            receivedPacketsA.push(packet);
            await wait(5);
          }
        })(),
        (async () => {
          for await (let packet of stream) {
            receivedPacketsB.push(packet);
            await wait(50);
          }
        })()
      ]);

      assert.equal(receivedPacketsA.length, 10);
      for (let i = 0; i < 10; i++) {
        assert.equal(receivedPacketsA[i], 'a' + i);
      }

      assert.equal(receivedPacketsB.length, 10);
      for (let i = 0; i < 10; i++) {
        assert.equal(receivedPacketsB[i], 'a' + i);
      }
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to resume consumption after the stream has been closed', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('a' + i);
        }
        stream.close();
      })();

      let receivedPacketsA = [];
      for await (let packet of stream) {
        receivedPacketsA.push(packet);
      }

      assert.equal(receivedPacketsA.length, 10);

      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('b' + i);
        }
        stream.close();
      })();

      let receivedPacketsB = [];
      for await (let packet of stream) {
        receivedPacketsB.push(packet);
      }

      assert.equal(receivedPacketsB.length, 10);
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to resume consumption of messages written within the same stack frame after the stream has been closed', async () => {
      stream.write('one');
      stream.write('two');

      let receivedPackets = [];

      let doneConsumingPromiseA = (async () => {
        for await (let packet of stream) {
          receivedPackets.push(packet);
        }
      })();

      stream.write('three');
      stream.write('four');
      stream.write('five');
      stream.close();

      await doneConsumingPromiseA;

      let doneConsumingPromiseB = (async () => {
        for await (let packet of stream) {
          receivedPackets.push(packet);
        }
      })();

      stream.write('six');
      stream.write('seven');
      stream.close();

      await doneConsumingPromiseB;

      assert.equal(receivedPackets.length, 5);
      assert.equal(receivedPackets[0], 'three');
      assert.equal(receivedPackets[1], 'four');
      assert.equal(receivedPackets[2], 'five');
      assert.equal(receivedPackets[3], 'six');
      assert.equal(receivedPackets[4], 'seven');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to optionally timeout the consumer iterator when write delay is consistent', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(30);
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterable = stream.createAsyncIterable(20);
      let error;
      try {
        for await (let packet of asyncIterable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 0);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to optionally timeout the consumer iterator when write delay is inconsistent', async () => {
      (async () => {
        await wait(10);
        stream.write('hello0');
        await wait(10);
        stream.write('hello1');
        await wait(10);
        stream.write('hello2');
        await wait(30);
        stream.write('hello3');
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterable = stream.createAsyncIterable(20);
      let error;
      try {
        for await (let packet of asyncIterable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 3);
      assert.equal(receivedPackets[0], 'hello0');
      assert.equal(receivedPackets[2], 'hello2');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to optionally timeout the consumer iterator even if steam is not explicitly closed', async () => {
      (async () => {
        await wait(10);
        stream.write('hello0');
        await wait(10);
        stream.write('hello1');
        await wait(30);
        stream.write('hello2');
      })();

      let receivedPackets = [];
      let asyncIterable = stream.createAsyncIterable(20);
      let error;
      try {
        for await (let packet of asyncIterable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 2);
      assert.equal(receivedPackets[0], 'hello0');
      assert.equal(receivedPackets[1], 'hello1');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to resume consumption immediately after stream is closed unless a condition is met', async () => {
      let resume = true;
      (async () => {
        for (let i = 0; i < 5; i++) {
          await wait(10);
          stream.write('hello' + i);
        }
        // Consumer should be able to resume without missing any messages.
        stream.close();
        stream.write('world0');
        for (let i = 1; i < 5; i++) {
          await wait(10);
          stream.write('world' + i);
        }
        resume = false;
        stream.close();
      })();

      let receivedPackets = [];
      let iterable = stream.createAsyncIterable();

      while (true) {
        for await (let data of iterable) {
          receivedPackets.push(data);
        }
        if (!resume) break;
      }

      assert.equal(receivedPackets.length, 10);
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to close stream with custom data', async () => {
      (async () => {
        for (let i = 0; i < 5; i++) {
          await wait(10);
          stream.write('hello' + i);
        }
        stream.close('done123');
      })();

      let receivedPackets = [];
      let receivedEndPacket = null;
      let iterator = stream.createAsyncIterator();

      while (true) {
        let packet = await iterator.next();
        if (packet.done) {
          receivedEndPacket = packet.value;
          break;
        }
        receivedPackets.push(packet.value);
      }

      assert.equal(receivedPackets.length, 5);
      assert.equal(receivedEndPacket, 'done123');
      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });
  });

  describe('backpressure', () => {
    beforeEach(async () => {
      stream = new WritableAsyncIterableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should track backpressure correctly when consuming stream', async () => {
      await Promise.all([
        (async () => {
          let backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 0);

          await wait(10);

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 0);

          stream.write('a0');

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 1);
          assert.equal(backpressureList[0].backpressure, 1);

          await wait(10);

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 0);

          stream.write('a1');

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 1);
          assert.equal(backpressureList[0].backpressure, 1);

          await wait(10);
          stream.write('a2');
          await wait(10);
          stream.write('a3');
          await wait(10);
          stream.write('a4');

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 1);
          assert.equal(backpressureList[0].backpressure, 4);

          stream.close();

          backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 1);
          assert.equal(backpressureList[0].backpressure, 5);
        })(),
        (async () => {
          let expectedPressure = 6;
          for await (let data of stream) {
            expectedPressure--;
            await wait(70);
            let backpressureList = stream.getBackpressureList();
            assert.equal(backpressureList.length, 1);
            assert.equal(backpressureList[0].backpressure, expectedPressure);
          }
          let backpressureList = stream.getBackpressureList();
          assert.equal(backpressureList.length, 0);
        })()
      ]);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should track backpressure correctly when consuming stream with an iterator', async () => {
      await Promise.all([
        (async () => {
          for (let i = 0; i < 10; i++) {
            await wait(10);
            stream.write('a' + i);
            let backpressureList = stream.getBackpressureList();
            assert.equal(backpressureList.length, 1);
            assert.equal(backpressureList[0].backpressure, i + 1);
          }
          stream.close();
        })(),
        (async () => {
          let iter = stream.createAsyncIterator();
          await wait(20);
          let expectedPressure = 11;
          while (true) {
            expectedPressure--;
            await wait(120);
            let data = await iter.next();
            let backpressureList = stream.getBackpressureList();

            if (data.done) {
              assert.equal(backpressureList.length, 0);
              break;
            }
            assert.equal(backpressureList.length, 1);
            assert.equal(backpressureList[0].backpressure, expectedPressure);
          }
        })()
      ]);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should track backpressure correctly when writing to and consuming stream intermittently with multiple iterators', async () => {
      let iterA = stream.createAsyncIterator();

      let backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 0);

      await wait(10);

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 0);

      stream.write('a0');

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 1);
      assert.equal(backpressureList[0].backpressure, 1);

      stream.write('a1');
      await wait(10);
      stream.write('a2');
      await wait(10);

      let iterB = stream.createAsyncIterator();

      stream.write('a3');
      await wait(10);
      stream.write('a4');

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 5);

      assert.equal(stream.getBackpressure(1), 5);
      assert.equal(stream.getBackpressure(2), 2);

      await iterA.next();

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 4);

      await iterA.next();
      await iterA.next();

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 2);

      stream.write('a5');
      stream.write('a6');
      stream.write('a7');

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 5);
      assert.equal(stream.getBackpressure(2), 5);

      stream.close();

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 6);

      await iterA.next();
      await iterA.next();
      await wait(10);
      await iterA.next();
      await iterA.next();
      await iterA.next();

      assert.equal(stream.getBackpressure(2), 6);

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 2);
      assert.equal(backpressureList[0].backpressure, 1);

      await iterB.next();
      await iterB.next();
      await iterB.next();
      await iterB.next();
      await iterB.next();

      assert.equal(stream.getBackpressure(2), 1);

      let iterBData = await iterB.next();

      assert.equal(stream.getBackpressure(2), 0);

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 1);
      assert.equal(backpressureList[0].backpressure, 1);

      let iterAData = await iterA.next();

      backpressureList = stream.getBackpressureList();
      assert.equal(backpressureList.length, 0);
      assert.equal(iterAData.done, true);
      assert.equal(iterBData.done, true);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });
  });

  describe('await once', () => {
    beforeEach(async () => {
      stream = new WritableAsyncIterableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
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

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should throw an error if a number is passed to the once() method and it times out', async () => {
      (async () => {
        for (let i = 0; i < 3; i++) {
          await wait(20);
          stream.write('a' + i);
        }
      })();

      let nextPacket = await stream.once(30);
      assert.equal(nextPacket, 'a0');

      let error;
      nextPacket = null;
      try {
        nextPacket = await stream.once(10);
      } catch (err) {
        error = err;
      }

      assert.equal(nextPacket, null);
      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should not resolve once() call when stream.close() is called', async () => {
      (async () => {
        await wait(10);
        stream.close();
      })();

      let receivedPackets = [];

      (async () => {
        let nextPacket = await stream.once();
        receivedPackets.push(nextPacket);
      })();

      await wait(100);
      assert.equal(receivedPackets.length, 0);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should not resolve previous once() call after stream.close() is called', async () => {
      (async () => {
        await wait(10);
        stream.close();
        await wait(10);
        stream.write('foo');
      })();

      let receivedPackets = [];

      (async () => {
        let nextPacket = await stream.once();
        receivedPackets.push(nextPacket);
      })();

      await wait(100);
      assert.equal(receivedPackets.length, 0);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should resolve once() if it is called after stream.close() is called and then a new packet is written', async () => {
      (async () => {
        await wait(10);
        stream.close();
        await wait(10);
        stream.write('foo');
      })();

      let receivedPackets = [];

      (async () => {
        let nextPacket = await stream.once();
        receivedPackets.push(nextPacket);
      })();

      await wait(100);

      assert.equal(receivedPackets.length, 0);

      (async () => {
        await wait(10);
        stream.write('bar');
      })();

      let packet = await stream.once();
      assert.equal(packet, 'bar');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });
  });

  describe('while loop with await inside', () => {
    beforeEach(async () => {
      stream = new WritableAsyncIterableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should receive packets asynchronously', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterator = stream.createAsyncIterator();
      while (true) {
        let packet = await asyncIterator.next();
        if (packet.done) break;
        receivedPackets.push(packet.value);
      }
      assert.equal(receivedPackets.length, 10);

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(10);
          stream.write('a' + i);
          stream.write('b' + i);
          stream.write('c' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterator = stream.createAsyncIterator();
      while (true) {
        let packet = await asyncIterator.next();
        if (packet.done) break;
        receivedPackets.push(packet.value);
      }
      assert.equal(receivedPackets.length, 30);
      assert.equal(receivedPackets[0], 'a0');
      assert.equal(receivedPackets[1], 'b0');
      assert.equal(receivedPackets[2], 'c0');
      assert.equal(receivedPackets[3], 'a1');
      assert.equal(receivedPackets[4], 'b1');
      assert.equal(receivedPackets[5], 'c1');
      assert.equal(receivedPackets[29], 'c9');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to timeout the iterator if the stream is idle for too long', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(30);
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterator = stream.createAsyncIterator(20);
      let error;
      try {
        while (true) {
          let packet = await asyncIterator.next();
          if (packet.done) break;
          receivedPackets.push(packet.value);
        }
      } catch (err) {
        error = err;
      }
      assert.equal(receivedPackets.length, 0);
      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });

    it('should be able to continue iterating if a single iteration times out', async () => {
      (async () => {
        await wait(20);
        stream.write('hello0');
        await wait(20);
        stream.write('hello1');
        await wait(40);
        stream.write('hello2');
        await wait(20);
        stream.write('hello3');
        await wait(20);
        stream.write('hello4');
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterator = stream.createAsyncIterator(30);
      let errors = [];

      while (true) {
        let packet;
        try {
          packet = await asyncIterator.next();
        } catch (err) {
          errors.push(err);
          continue;
        }
        if (packet.done) break;
        receivedPackets.push(packet.value);
      }
      assert.equal(receivedPackets.length, 5);
      assert.equal(errors.length, 1);
      assert.notEqual(errors[0], null);
      assert.equal(errors[0].name, 'TimeoutError');

      assert.equal(Object.keys(stream._consumers).length, 0); // Check internal cleanup.
    });
  });
});
