const WritableConsumableStream = require('../index');
const assert = require('assert');

let pendingTimeoutSet = new Set();

function wait(duration) {
  return new Promise((resolve) => {
    let timeout = setTimeout(() => {
      pendingTimeoutSet.delete(timeout);
      resolve();
    }, duration);
    pendingTimeoutSet.add(timeout);
  });
}

async function consumeAllPackets(consumer) {
  let packets = [];
  while (true) {
    let packet = await consumer.next();
    packets.push(packet);
    if (packet.done) break;
  }
  return packets;
}

function withTimeout(promise, duration, message) {
  return Promise.race([
    promise,
    (async () => {
      await wait(duration);
      throw new Error(message);
    })()
  ]);
}

function cancelAllPendingWaits() {
  for (let timeout of pendingTimeoutSet) {
    clearTimeout(timeout);
  }
  pendingTimeoutSet.clear();
}

describe('WritableConsumableStream', () => {
  let stream;

  describe('for-await-of loop', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let consumable = stream.createConsumer(20);
      let error;
      try {
        for await (let packet of consumable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      let consumerStatsList = stream.getConsumerStatsList();
      assert.equal(consumerStatsList.length, 0);

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 0);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let consumable = stream.createConsumer(20);
      let error;
      try {
        for await (let packet of consumable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      let consumerStatsList = stream.getConsumerStatsList();
      assert.equal(consumerStatsList.length, 0);

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 3);
      assert.equal(receivedPackets[0], 'hello0');
      assert.equal(receivedPackets[2], 'hello2');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let consumable = stream.createConsumer(20);
      let error;
      try {
        for await (let packet of consumable) {
          receivedPackets.push(packet);
        }
      } catch (err) {
        error = err;
      }

      let consumerStatsList = stream.getConsumerStatsList();
      assert.equal(consumerStatsList.length, 0);

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
      assert.equal(receivedPackets.length, 2);
      assert.equal(receivedPackets[0], 'hello0');
      assert.equal(receivedPackets[1], 'hello1');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let consumable = stream.createConsumer();

      while (true) {
        for await (let data of consumable) {
          receivedPackets.push(data);
        }
        if (!resume) break;
      }

      assert.equal(receivedPackets.length, 10);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let consumer = stream.createConsumer();

      while (true) {
        let packet = await consumer.next();
        if (packet.done) {
          receivedEndPacket = packet.value;
          break;
        }
        receivedPackets.push(packet.value);
      }

      assert.equal(receivedPackets.length, 5);
      assert.equal(receivedEndPacket, 'done123');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });
  });

  describe('kill', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should stop consumer immediately when stream is killed', async () => {
      let backpressureBeforeKill;
      let backpressureAfterKill;
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        backpressureBeforeKill = stream.getBackpressure();
        stream.kill();
        backpressureAfterKill = stream.getBackpressure();
      })();

      let receivedPackets = [];
      for await (let packet of stream) {
        await wait(50);
        receivedPackets.push(packet);
      }

      let backpressureAfterConsume = stream.getBackpressure();

      assert.equal(backpressureBeforeKill, 10);
      assert.equal(backpressureAfterKill, 0);
      assert.equal(backpressureAfterConsume, 0);
      assert.equal(receivedPackets.length, 0);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should throw a timeout error early when stream is killed', async () => {
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        stream.kill();
      })();

      let error;
      try {
        await Promise.race([
          stream.once(200), // This should throw an error early.
          wait(100)
        ]);
      } catch (err) {
        error = err;
      }

      assert.equal(error.name, 'TimeoutError');

      let backpressure = stream.getBackpressure();
      assert.equal(backpressure, 0);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should be able to restart a killed stream', async () => {
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        await wait(10);
        stream.kill();

        await wait(70);

        for (let i = 0; i < 10; i++) {
          stream.write('world' + i);
        }
        await wait(10);
        stream.close();
      })();

      let receivedPackets = [];
      for await (let packet of stream) {
        await wait(50);
        receivedPackets.push(packet);
      }

      for await (let packet of stream) {
        await wait(50);
        receivedPackets.push(packet);
      }

      let backpressure = stream.getBackpressure();
      assert.equal(backpressure, 0);

      assert.equal(receivedPackets.length, 11);
      assert.equal(receivedPackets[0], 'hello0');
      assert.equal(receivedPackets[1], 'world0');
      assert.equal(receivedPackets[10], 'world9');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should be able to start writing to a killed stream immediately', async () => {
      (async () => {
        await wait(10);
        stream.kill();
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let consumer = stream.createConsumer();

      let receivedPackets = [];
      while (true) {
        let packet = await consumer.next();
        if (packet.done) break;
        receivedPackets.push(packet);
      }
      while (true) {
        let packet = await consumer.next();
        if (packet.done) break;
        receivedPackets.push(packet);
      }
      assert.equal(receivedPackets.length, 10);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should set consumer.isAlive to false if stream is killed', async () => { // TODO 22
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let consumer = stream.createConsumer();
      assert.equal(consumer.isAlive, true);
      stream.kill();
      assert.equal(consumer.isAlive, false);

      let receivedPackets = [];
      while (true) {
        let packet = await consumer.next();
        if (packet.done) break;
        receivedPackets.push(packet);
      }
      assert.equal(receivedPackets.length, 0);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should pass kill data to consumer when stream is killed if using consumer', async () => {
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        stream.kill(12345);
      })();

      let consumer = stream.createConsumer();
      let receivedPackets = [];
      while (true) {
        let packet = await consumer.next();
        await wait(50);
        receivedPackets.push(packet);
        if (packet.done) {
          break;
        }
      }
      assert.equal(receivedPackets.length, 1);
      assert.equal(receivedPackets[0].value, 12345);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should stop consumer at the end of the current iteration when stream is killed and iteration has already started', async () => {
      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        await wait(10);
        stream.kill();
      })();
      let receivedPackets = [];
      for await (let packet of stream) {
        await wait(50);
        receivedPackets.push(packet);
      }
      assert.equal(receivedPackets.length, 1);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should stop all consumers immediately', async () => {
      let isWriting = true;

      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(40);
          if (!isWriting) return;
          stream.write('a' + i);
        }
      })();

      (async () => {
        await wait(220);
        stream.kill();
        isWriting = false;
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.all([
        (async () => {
          for await (let packet of stream) {
            receivedPacketsA.push(packet);
          }
        })(),
        (async () => {
          for await (let packet of stream) {
            receivedPacketsB.push(packet);
            await wait(300);
          }
        })()
      ]);

      assert.equal(receivedPacketsA.length, 5);
      assert.equal(receivedPacketsB.length, 1);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should stop consumers which have not started iterating', async () => {
      let consumer = stream.createConsumer();

      for (let i = 0; i < 10; i++) {
        stream.write('hello' + i);
      }

      stream.kill('end');

      await wait(10);

      assert.equal(consumer.getBackpressure(), 0);
    });

    it('should deliver the kill packet if the consumer was killed while it was not waiting for a packet', async () => {
      let consumer = stream.createConsumer();
      stream.write('a');

      let firstPacket = await consumer.next();

      // The consumer is now between iterations rather than suspended
      // inside next(), which is when kill() used to be swallowed.
      stream.killConsumer(consumer.id, 'custom kill data');

      let secondPacket = await withTimeout(
        consumer.next(),
        200,
        'next() never resolved after the consumer was killed'
      );

      assert.equal(firstPacket.value, 'a');
      assert.equal(secondPacket.done, true);
      assert.equal(secondPacket.value, 'custom kill data');
      assert.equal(stream.getConsumerCount(), 0);
    });

    it('should invoke removeConsumerCallback once per consumer removal', async () => {
      let removedConsumerIds = [];
      let callbackStream = new WritableConsumableStream({
        removeConsumerCallback: (consumerId) => removedConsumerIds.push(consumerId)
      });

      let consumer = callbackStream.createConsumer();

      let packetPromise = consumer.next();
      await wait(10);

      // kill() destroys the consumer, then next() destroys it again when it
      // reads the kill packet; the callback must only fire for the first one.
      callbackStream.kill('custom kill data');

      let packet = await withTimeout(packetPromise, 200, 'next() never settled after kill');

      assert.equal(packet.done, true);
      assert.deepEqual(removedConsumerIds, [consumer.id]);
      assert.equal(callbackStream.getConsumerCount(), 0);
    });

    it('should report a consumer as alive again once it resumes consuming after a kill', async () => {
      let consumer = stream.createConsumer();
      stream.write('a');

      let firstPacket = await consumer.next();

      assert.equal(firstPacket.value, 'a');
      assert.equal(consumer.isAlive, true);

      stream.killConsumer(consumer.id);

      let killPacket = await consumer.next();

      assert.equal(killPacket.done, true);
      assert.equal(consumer.isAlive, false);

      // Consumers are revivable, so a consumer which resumes must not keep
      // reporting itself as dead.
      stream.write('b');

      let resumedPacket = await consumer.next();

      assert.equal(resumedPacket.value, 'b');
      assert.equal(consumer.isAlive, true);
      assert.equal(stream.hasConsumer(consumer.id), true);
    });

    it('should not leave a killed consumer registered on the stream after it calls next()', async () => {
      let consumer = stream.createConsumer();

      stream.killConsumer(consumer.id, 'custom kill data');

      assert.equal(stream.getConsumerCount(), 0);

      let packet = await withTimeout(
        consumer.next(),
        200,
        'next() never resolved after the consumer was killed'
      );

      assert.equal(packet.done, true);
      assert.equal(consumer.isAlive, false);
      // A killed consumer may briefly re-register itself inside next()
      // (consumers are revivable by design), but it must not stay registered.
      assert.equal(stream.getConsumerCount(), 0);
    });
  });

  describe('backpressure', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should track backpressure correctly when consuming stream', async () => {
      await Promise.all([
        (async () => {
          let consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 0);

          await wait(10);

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 0);
          assert.equal(consumerStats[0].id, 1);

          assert.equal(stream.hasConsumer(1), true);
          assert.equal(stream.hasConsumer(2), false);

          stream.write('a0');

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 1);

          await wait(10);

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 0);

          stream.write('a1');

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 1);

          await wait(10);
          stream.write('a2');
          await wait(10);
          stream.write('a3');
          await wait(10);
          stream.write('a4');

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 4);

          stream.close();

          consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 1);
          assert.equal(consumerStats[0].backpressure, 5);
        })(),
        (async () => {
          let expectedPressure = 6;
          for await (let data of stream) {
            expectedPressure--;
            await wait(70);
            let consumerStats = stream.getConsumerStatsList();
            assert.equal(consumerStats.length, 1);
            assert.equal(consumerStats[0].backpressure, expectedPressure);
          }
          let consumerStats = stream.getConsumerStatsList();
          assert.equal(consumerStats.length, 0);
        })()
      ]);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should track backpressure correctly when consuming stream with a consumer', async () => {
      await Promise.all([
        (async () => {
          for (let i = 0; i < 10; i++) {
            await wait(10);
            stream.write('a' + i);
            let consumerStats = stream.getConsumerStatsList();
            assert.equal(consumerStats.length, 1);
            assert.equal(consumerStats[0].backpressure, i + 1);
          }
          stream.close();
        })(),
        (async () => {
          let iter = stream.createConsumer();
          assert.equal(iter.id, 1);

          await wait(20);
          let expectedPressure = 11;
          while (true) {
            expectedPressure--;
            await wait(120);
            let data = await iter.next();
            let consumerStats = stream.getConsumerStatsList();

            if (data.done) {
              assert.equal(consumerStats.length, 0);
              break;
            }
            assert.equal(consumerStats.length, 1);
            assert.equal(consumerStats[0].backpressure, expectedPressure);
          }
        })()
      ]);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should track backpressure correctly when writing to and consuming stream intermittently with multiple consumers', async () => {
      let iterA = stream.createConsumer();
      assert.equal(iterA.id, 1);

      let consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 1);
      assert.equal(consumerStats[0].backpressure, 0);

      await wait(10);

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 1);
      assert.equal(consumerStats[0].backpressure, 0);

      stream.write('a0');

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 1);
      assert.equal(consumerStats[0].backpressure, 1);

      stream.write('a1');
      await wait(10);
      stream.write('a2');
      await wait(10);

      let iterB = stream.createConsumer();
      assert.equal(iterB.id, 2);

      stream.write('a3');
      await wait(10);
      stream.write('a4');

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 5);

      assert.equal(stream.getBackpressure(), 5);

      assert.equal(iterA.getBackpressure(), 5);
      assert.equal(stream.getConsumerBackpressure(1), 5);
      assert.equal(iterB.getBackpressure(), 2);
      assert.equal(stream.getConsumerBackpressure(2), 2);

      await iterA.next();

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 4);

      await iterA.next();
      await iterA.next();

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 2);

      stream.write('a5');
      stream.write('a6');
      stream.write('a7');

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 5);
      assert.equal(stream.getConsumerBackpressure(2), 5);

      stream.close();

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 6);

      assert.equal(stream.getBackpressure(), 6);

      await iterA.next();
      await iterA.next();
      await wait(10);
      await iterA.next();
      await iterA.next();
      await iterA.next();

      assert.equal(stream.getConsumerBackpressure(2), 6);

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 2);
      assert.equal(consumerStats[0].backpressure, 1);

      await iterB.next();
      await iterB.next();
      await iterB.next();
      await iterB.next();
      await iterB.next();

      assert.equal(stream.getConsumerBackpressure(2), 1);

      let iterBData = await iterB.next();

      assert.equal(stream.getConsumerBackpressure(2), 0);

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 1);
      assert.equal(consumerStats[0].backpressure, 1);

      assert.equal(stream.getBackpressure(), 1);

      let iterAData = await iterA.next();

      consumerStats = stream.getConsumerStatsList();
      assert.equal(consumerStats.length, 0);
      assert.equal(iterAData.done, true);
      assert.equal(iterBData.done, true);

      assert.equal(iterA.getBackpressure(), 0);
      assert.equal(iterB.getBackpressure(), 0);

      assert.equal(stream.getBackpressure(), 0);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should reset backpressure after invoking consumer.return()', async () => {
      let consumer = stream.createConsumer();

      for (let i = 0; i < 10; i++) {
        stream.write('hello' + i);
      }
      stream.close('end');

      await wait(10);
      consumer.return();

      assert.equal(stream.getConsumerStatsList().length, 0);
      assert.equal(consumer.getBackpressure(), 0);

      for (let i = 0; i < 10; i++) {
        stream.write('hi' + i);
      }
      stream.close('end');

      await wait(10);

      assert.equal(stream.getConsumerStatsList().length, 0);
      assert.equal(consumer.getBackpressure(), 0);

      consumer.return();

      assert.equal(stream.getConsumerStatsList().length, 0);
      assert.equal(consumer.getBackpressure(), 0);
    });

    it('should be able to calculate correct backpressure after invoking consumer.return()', async () => {
      let consumer = stream.createConsumer();

      for (let i = 0; i < 10; i++) {
        stream.write('hello' + i);
      }

      stream.close('end');

      await wait(10);
      consumer.return();

      assert.equal(stream.getConsumerStatsList().length, 0);
      assert.equal(consumer.getBackpressure(), 0);

      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hi' + i);
        }
        stream.close('end');
      })();

      let receivedPackets = [];

      let expectedPressure = 10;
      while (true) {
        let packet = await consumer.next();
        assert.equal(consumer.getBackpressure(), expectedPressure);
        let consumerStatsList = stream.getConsumerStatsList();
        if (expectedPressure > 0) {
          assert.equal(consumerStatsList.length, 1);
          assert.equal(consumerStatsList[0].backpressure, expectedPressure);
        } else {
          assert.equal(consumerStatsList.length, 0);
        }
        expectedPressure--;
        receivedPackets.push(packet);
        if (packet.done) break;
      }

      assert.equal(receivedPackets.length, 11);
      assert.equal(receivedPackets[0].value, 'hi0');
      assert.equal(receivedPackets[9].value, 'hi9');
      assert.equal(receivedPackets[10].done, true);
      assert.equal(receivedPackets[10].value, 'end');

      assert.equal(stream.getConsumerStatsList().length, 0);
      assert.equal(consumer.getBackpressure(), 0);
    });
  });

  describe('await once', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });
  });

  describe('while loop with await inside', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
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
      let asyncIterator = stream.createConsumer();
      while (true) {
        let packet = await asyncIterator.next();
        if (packet.done) break;
        receivedPackets.push(packet.value);
      }
      assert.equal(receivedPackets.length, 10);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let asyncIterator = stream.createConsumer();
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should be able to timeout the consumer if the stream is idle for too long', async () => {
      (async () => {
        for (let i = 0; i < 10; i++) {
          await wait(30);
          stream.write('hello' + i);
        }
        stream.close();
      })();

      let receivedPackets = [];
      let asyncIterator = stream.createConsumer(20);
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
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
      let asyncIterator = stream.createConsumer(30);
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

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });
  });

  describe('actions on an individual consumer', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should stop a specific consumer immediately when that consumer is killed', async () => {
      let backpressureBeforeKill;
      let backpressureAfterKill;

      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        backpressureBeforeKill = stream.getBackpressure();
        stream.killConsumer(consumerA.id, 'custom kill data');
        backpressureAfterKill = stream.getBackpressure();
        stream.close();
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.all([
        (async () => {
          while (true) {
            let packet = await consumerA.next();
            await wait(50);
            receivedPacketsA.push(packet);
            if (packet.done) break;
          }
        })(),
        (async () => {
          while (true) {
            let packet = await consumerB.next();
            await wait(50);
            receivedPacketsB.push(packet);
            if (packet.done) break;
          }
        })()
      ]);

      let backpressureAfterConsume = stream.getBackpressure();

      assert.equal(backpressureBeforeKill, 10);
      assert.equal(backpressureAfterKill, 10); // consumerB was still running.
      assert.equal(backpressureAfterConsume, 0);
      assert.equal(receivedPacketsA.length, 1);
      assert.equal(receivedPacketsA[0].done, true);
      assert.equal(receivedPacketsA[0].value, 'custom kill data');
      assert.equal(receivedPacketsB.length, 11);
      assert.equal(receivedPacketsB[0].value, 'hello0');
      assert.equal(receivedPacketsB[9].value, 'hello9');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should stop a specific consumer when that consumer is closed', async () => {
      let maxBackpressureBeforeClose;
      let maxBackpressureAfterClose;
      let backpressureBeforeCloseA;
      let backpressureBeforeCloseB;

      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        maxBackpressureBeforeClose = stream.getBackpressure();
        stream.closeConsumer(consumerA.id, 'custom close data');
        maxBackpressureAfterClose = stream.getBackpressure();
        stream.write('foo');
        backpressureBeforeCloseA = stream.getConsumerBackpressure(consumerA.id);
        backpressureBeforeCloseB = stream.getConsumerBackpressure(consumerB.id);
        stream.close('close others');
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.all([
        (async () => {
          while (true) {
            let packet = await consumerA.next();
            await wait(50);
            receivedPacketsA.push(packet);
            if (packet.done) break;
          }
        })(),
        (async () => {
          while (true) {
            let packet = await consumerB.next();
            await wait(50);
            receivedPacketsB.push(packet);
            if (packet.done) break;
          }
        })()
      ]);

      let maxBackpressureAfterConsume = stream.getBackpressure();

      assert.equal(backpressureBeforeCloseA, 12);
      // consumerB is not charged for the packet which closeConsumer()
      // addressed to consumerA; that node is on the shared queue but it is
      // not part of consumerB's backlog.
      assert.equal(backpressureBeforeCloseB, 11);
      assert.equal(maxBackpressureBeforeClose, 10);
      assert.equal(maxBackpressureAfterClose, 11);
      assert.equal(maxBackpressureAfterConsume, 0);
      assert.equal(receivedPacketsA.length, 11);
      assert.equal(receivedPacketsA[0].value, 'hello0');
      assert.equal(receivedPacketsA[9].value, 'hello9');
      assert.equal(receivedPacketsA[10].done, true);
      assert.equal(receivedPacketsA[10].value, 'custom close data');
      assert.equal(receivedPacketsB.length, 12);
      assert.equal(receivedPacketsB[0].value, 'hello0');
      assert.equal(receivedPacketsB[9].value, 'hello9');
      assert.equal(receivedPacketsB[10].value, 'foo');
      assert.equal(receivedPacketsB[11].done, true);
      assert.equal(receivedPacketsB[11].value, 'close others');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should support closing only one of multiple consumers', async () => {
      let backpressureBeforeClose;
      let backpressureAfterClose;

      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        backpressureBeforeClose = stream.getBackpressure();
        stream.closeConsumer(consumerA.id, 'custom close data');
        backpressureAfterClose = stream.getBackpressure();
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.race([
        (async () => {
          while (true) {
            let packet = await consumerA.next();
            await wait(50);
            receivedPacketsA.push(packet);
            if (packet.done) break;
          }
        })(),
        (async () => {
          while (true) {
            let packet = await consumerB.next();
            await wait(50);
            receivedPacketsB.push(packet);
            if (packet.done) break;
          }
        })()
      ]);

      let backpressureAfterConsume = stream.getBackpressure();

      assert.equal(backpressureBeforeClose, 10);
      assert.equal(backpressureAfterClose, 11);
      assert.equal(backpressureAfterConsume, 0);
      assert.equal(receivedPacketsA.length, 11);
      assert.equal(receivedPacketsA[10].done, true);
      assert.equal(receivedPacketsA[10].value, 'custom close data');
      assert.equal(receivedPacketsB.length, 10);
      assert.equal(receivedPacketsB[0].value, 'hello0');
      assert.equal(receivedPacketsB[9].value, 'hello9');

      stream.closeConsumer(consumerB.id);
      await wait(10);

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should be able to write to a specific consumer', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      (async () => {
        await wait(10);
        for (let i = 0; i < 10; i++) {
          stream.write('hello' + i);
        }
        for (let i = 0; i < 3; i++) {
          stream.writeToConsumer(consumerA.id, 'hi' + i);
        }
        stream.close('close all');
      })();

      let receivedPacketsA = [];
      let receivedPacketsB = [];

      await Promise.all([
        (async () => {
          while (true) {
            let packet = await consumerA.next();
            await wait(50);
            receivedPacketsA.push(packet);
            if (packet.done) break;
          }
        })(),
        (async () => {
          while (true) {
            let packet = await consumerB.next();
            await wait(50);
            receivedPacketsB.push(packet);
            if (packet.done) break;
          }
        })()
      ]);

      let backpressureAfterConsume = stream.getBackpressure();

      assert.equal(backpressureAfterConsume, 0);
      assert.equal(receivedPacketsA.length, 14);
      assert.equal(receivedPacketsA[0].value, 'hello0');
      assert.equal(receivedPacketsA[9].value, 'hello9');
      assert.equal(receivedPacketsA[10].value, 'hi0');
      assert.equal(receivedPacketsA[12].value, 'hi2');
      assert.equal(receivedPacketsA[13].done, true);
      assert.equal(receivedPacketsA[13].value, 'close all');
      assert.equal(receivedPacketsB.length, 11);
      assert.equal(receivedPacketsB[0].value, 'hello0');
      assert.equal(receivedPacketsB[9].value, 'hello9');
      assert.equal(receivedPacketsB[10].done, true);
      assert.equal(receivedPacketsB[10].value, 'close all');

      assert.equal(stream.getConsumerCount(), 0); // Check internal cleanup.
    });

    it('should not deliver a targeted packet to other consumers when the consumer id is falsy but valid', async () => {
      let consumerIds = [0, ''];
      let index = 0;
      let falsyIdStream = new WritableConsumableStream({
        generateConsumerId: () => consumerIds[index++]
      });
      let consumerA = falsyIdStream.createConsumer(); // id 0
      let consumerB = falsyIdStream.createConsumer(); // id ''

      falsyIdStream.writeToConsumer(consumerA.id, 'only for A');
      falsyIdStream.close('close all');

      let receivedPacketsA = await consumeAllPackets(consumerA);
      let receivedPacketsB = await consumeAllPackets(consumerB);

      assert.equal(receivedPacketsA.length, 2);
      assert.equal(receivedPacketsA[0].value, 'only for A');
      assert.equal(receivedPacketsA[1].done, true);

      // consumerB must never see the packet addressed to consumerA.
      assert.equal(receivedPacketsB.length, 1);
      assert.equal(receivedPacketsB[0].done, true);
      assert.equal(receivedPacketsB[0].value, 'close all');
    });

    it('should throw and write nothing if writeToConsumer is given a null or undefined consumer id', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      for (let badConsumerId of [undefined, null]) {
        assert.throws(() => {
          stream.writeToConsumer(badConsumerId, 'private message');
        }, TypeError);
        assert.throws(() => {
          stream.closeConsumer(badConsumerId, 'private close');
        }, TypeError);
      }

      assert.equal(stream.getBackpressure(), 0); // Nothing was appended to the stream.

      stream.close('close all');

      let receivedPacketsA = await consumeAllPackets(consumerA);
      let receivedPacketsB = await consumeAllPackets(consumerB);

      // Neither consumer saw the rejected 'private message' packet.
      assert.equal(receivedPacketsA.length, 1);
      assert.equal(receivedPacketsA[0].value, 'close all');
      assert.equal(receivedPacketsB.length, 1);
      assert.equal(receivedPacketsB[0].value, 'close all');
    });

    it('should terminate a pending next() call when return() is invoked', async () => {
      let consumer = stream.createConsumer();

      // The consumer is suspended inside next() with nothing to read.
      let packetPromise = consumer.next();
      await wait(10);

      consumer.return();

      let packet = await withTimeout(
        packetPromise,
        200,
        'next() never settled after return() was invoked'
      );

      assert.equal(packet.done, true);
      assert.equal(stream.getConsumerCount(), 0);
    });

    it('should clear the active timeout when return() is invoked', async () => {
      let consumer = stream.createConsumer(50);

      let packetPromise = consumer.next();
      await wait(10);

      consumer.return();

      let packet = await withTimeout(
        packetPromise,
        200,
        'next() never settled after return() was invoked'
      );

      assert.equal(packet.done, true);
      // No dangling timer may be left behind to fire at a dead consumer.
      assert.equal(consumer._timeoutId, undefined);

      await wait(100); // Past the original 50ms timeout.

      assert.equal(stream.getConsumerCount(), 0);
    });
  });

  describe('consumer count', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should return the number of consumers as 1 after stream.createConsumer() is used once', async () => {
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      assert.equal(stream.getConsumerCount(), 1);
    });

    it('should return the number of consumers as 2 after stream.createConsumer() is used twice', async () => {
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      assert.equal(stream.getConsumerCount(), 2);
    });

    it('should return the number of consumers as 1 after stream is consumed directly by for-await-of loop once', async () => {
      (async () => {
        for await (let message of stream) {}
      })();

      assert.equal(stream.getConsumerCount(), 1);
    });

    it('should return the number of consumers as 2 after stream is consumed directly by for-await-of loop twice', async () => {
      (async () => {
        for await (let message of stream) {}
      })();

      (async () => {
        for await (let message of stream) {}
      })();

      assert.equal(stream.getConsumerCount(), 2);
    });

    it('should return the number of consumers as 0 after stream is killed', async () => {
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      stream.kill();

      assert.equal(stream.getConsumerCount(), 0);
    });

    it('should return the number of consumers as 1 after a specific consumer is killed', async () => {
      let consumerA;
      (async () => {
        consumerA = stream.createConsumer();
        for await (let message of consumerA) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      stream.killConsumer(consumerA.id);

      assert.equal(stream.getConsumerCount(), 1);
    });

    it('should return the number of consumers as 0 after stream is close after last message has been consumed', async () => {
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      stream.close();

      assert.equal(stream.getConsumerCount(), 3);

      await wait(1000);

      assert.equal(stream.getConsumerCount(), 0);
    });

    it('should return the number of consumers as 1 after a specific consumer is closed after last message has been consumed', async () => {
      let consumerA;
      (async () => {
        consumerA = stream.createConsumer();
        for await (let message of consumerA) {}
      })();
      (async () => {
        for await (let message of stream.createConsumer()) {}
      })();

      stream.closeConsumer(consumerA.id);

      assert.equal(stream.getConsumerCount(), 2);

      await wait(1000);

      assert.equal(stream.getConsumerCount(), 1);
    });
  });

  describe('per-consumer backpressure attribution', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should not charge a consumer for packets which were addressed to a different consumer', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      for (let i = 0; i < 10; i++) {
        stream.writeToConsumer(consumerA.id, 'for-a' + i);
      }

      assert.equal(stream.getConsumerBackpressure(consumerA.id), 10);
      assert.equal(stream.getConsumerBackpressure(consumerB.id), 0);
      assert.equal(stream.getBackpressure(), 10);
    });

    it('should charge every consumer for broadcast packets', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      stream.write('hello');
      stream.writeToConsumer(consumerA.id, 'for-a');
      stream.write('world');

      assert.equal(stream.getConsumerBackpressure(consumerA.id), 3);
      assert.equal(stream.getConsumerBackpressure(consumerB.id), 2);
    });

    it('should return to zero backpressure after mixed broadcast and targeted traffic is consumed', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      let receivedA = [];
      let receivedB = [];

      let consumeA = (async () => {
        for await (let packet of consumerA) {
          receivedA.push(packet);
        }
      })();
      let consumeB = (async () => {
        for await (let packet of consumerB) {
          receivedB.push(packet);
        }
      })();

      await wait(10);

      for (let i = 0; i < 20; i++) {
        stream.write('broadcast' + i);
        stream.writeToConsumer(consumerA.id, 'for-a' + i);
        stream.writeToConsumer(consumerB.id, 'for-b' + i);
      }

      stream.close();
      await Promise.all([consumeA, consumeB]);

      assert.equal(receivedA.length, 40);
      assert.equal(receivedB.length, 40);
      assert.equal(consumerA.getBackpressure(), 0);
      assert.equal(consumerB.getBackpressure(), 0);
      assert.equal(stream.getBackpressure(), 0);
    });

    it('should preserve the order of broadcast and targeted packets', async () => {
      let consumer = stream.createConsumer();

      stream.write(1);
      stream.writeToConsumer(consumer.id, 2);
      stream.write(3);
      stream.writeToConsumer(consumer.id, 4);
      stream.close();

      let packets = await consumeAllPackets(consumer);
      let values = packets.filter(packet => !packet.done).map(packet => packet.value);

      assert.equal(JSON.stringify(values), JSON.stringify([1, 2, 3, 4]));
    });

    it('should not deliver targeted packets to other consumers', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      stream.writeToConsumer(consumerA.id, 'secret-a');
      stream.write('public');
      stream.writeToConsumer(consumerB.id, 'secret-b');
      stream.close();

      let packetsA = await consumeAllPackets(consumerA);
      let packetsB = await consumeAllPackets(consumerB);

      let valuesA = packetsA.filter(packet => !packet.done).map(packet => packet.value);
      let valuesB = packetsB.filter(packet => !packet.done).map(packet => packet.value);

      assert.equal(JSON.stringify(valuesA), JSON.stringify(['secret-a', 'public']));
      assert.equal(JSON.stringify(valuesB), JSON.stringify(['public', 'secret-b']));
    });

    it('should not reset a consumer timeout when a packet is addressed to a different consumer', async () => {
      let victim = stream.createConsumer(100);
      let other = stream.createConsumer();

      let error;
      let consumeVictim = (async () => {
        try {
          for await (let packet of victim) {}
        } catch (err) {
          error = err;
        }
      })();

      // Continuously write packets addressed only to the other consumer; this
      // must not keep the victim's inactivity timeout alive.
      let spam = (async () => {
        for (let i = 0; i < 15; i++) {
          stream.writeToConsumer(other.id, 'spam' + i);
          await wait(20);
        }
      })();

      await Promise.all([consumeVictim, spam]);

      assert.notEqual(error, null);
      assert.equal(error.name, 'TimeoutError');
    });

    it('should still reset a consumer timeout when a broadcast packet arrives', async () => {
      let consumer = stream.createConsumer(100);

      let error;
      let received = [];
      let consume = (async () => {
        try {
          for await (let packet of consumer) {
            received.push(packet);
          }
        } catch (err) {
          error = err;
        }
      })();

      for (let i = 0; i < 5; i++) {
        await wait(20);
        stream.write('hello' + i);
      }
      stream.close();

      await consume;

      assert.equal(error, null);
      assert.equal(received.length, 5);
    });
  });

  describe('queue depth', () => {
    beforeEach(async () => {
      stream = new WritableConsumableStream();
    });

    afterEach(async () => {
      cancelAllPendingWaits();
      stream.close();
    });

    it('should report how many nodes of the shared queue a consumer is still pinning', async () => {
      let idle = stream.createConsumer();
      let active = stream.createConsumer();

      let consumeActive = (async () => {
        for await (let packet of active) {}
      })();

      await wait(10);

      for (let i = 0; i < 10; i++) {
        stream.writeToConsumer(active.id, 'for-active' + i);
      }

      // The idle consumer owes no work, but it still pins every node.
      assert.equal(stream.getConsumerBackpressure(idle.id), 0);
      assert.equal(stream.getConsumerQueueDepth(idle.id), 10);
      assert.equal(stream.getQueueDepth(), 10);

      await wait(50);

      // Once the active consumer has drained, it owes no more work, but the
      // queue is still holding every node on the idle consumer's behalf - and
      // stream-level backpressure reports that, because it measures the queue.
      assert.equal(stream.getConsumerBackpressure(active.id), 0);
      assert.equal(stream.getConsumerBackpressure(idle.id), 0);
      assert.equal(stream.getQueueDepth(), 10);
      assert.equal(stream.getBackpressure(), 10);

      stream.close();
      await consumeActive;
    });

    it('should count packets addressed to any consumer towards stream backpressure', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      stream.write('broadcast');
      for (let i = 0; i < 5; i++) {
        stream.writeToConsumer(consumerA.id, 'spam-a' + i);
        stream.writeToConsumer(consumerB.id, 'spam-b' + i);
      }

      // Neither consumer owes more than 6 packets of work, but the queue is
      // pinning all 11 nodes; stream backpressure must reflect the queue so
      // that it can be used to limit spam.
      assert.equal(stream.getConsumerBackpressure(consumerA.id), 6);
      assert.equal(stream.getConsumerBackpressure(consumerB.id), 6);
      assert.equal(stream.getBackpressure(), 11);
    });

    it('should not pin queue nodes for an idle consumer which is waiting inside next()', async () => {
      let idle = stream.createConsumer();
      let active = stream.createConsumer();

      let consumeIdle = (async () => {
        for await (let packet of idle) {}
      })();
      let consumeActive = (async () => {
        for await (let packet of active) {}
      })();

      await wait(10);

      for (let i = 0; i < 100; i++) {
        stream.writeToConsumer(active.id, 'for-active' + i);
      }

      await wait(50);

      // The idle consumer is parked at the tail of the queue with nothing of
      // its own pending, so it must not pin the nodes which were addressed to
      // the active consumer; otherwise the queue would grow without bound even
      // though no consumer has any work left to do.
      assert.equal(stream.getConsumerQueueDepth(idle.id), 0);
      assert.equal(stream.getQueueDepth(), 0);
      assert.equal(stream.getBackpressure(), 0);

      stream.close();
      await Promise.all([consumeIdle, consumeActive]);
    });

    it('should still deliver later packets to an idle consumer which skipped ahead', async () => {
      let idle = stream.createConsumer();
      let active = stream.createConsumer();

      let receivedIdle = [];
      let consumeIdle = (async () => {
        for await (let packet of idle) {
          receivedIdle.push(packet);
        }
      })();
      let consumeActive = (async () => {
        for await (let packet of active) {}
      })();

      await wait(10);

      for (let i = 0; i < 10; i++) {
        stream.writeToConsumer(active.id, 'for-active' + i);
      }

      await wait(20);

      stream.write('broadcast');
      stream.writeToConsumer(idle.id, 'for-idle');

      await wait(20);

      assert.equal(JSON.stringify(receivedIdle), JSON.stringify(['broadcast', 'for-idle']));

      stream.close();
      await Promise.all([consumeIdle, consumeActive]);
    });

    it('should report zero queue depth for a stream with no backlog', async () => {
      let consumer = stream.createConsumer();

      assert.equal(stream.getQueueDepth(), 0);
      assert.equal(stream.getConsumerQueueDepth(consumer.id), 0);
    });

    it('should report zero queue depth for an unknown consumer', async () => {
      assert.equal(stream.getConsumerQueueDepth(12345), 0);
    });

    it('should drop back to zero queue depth once every consumer has caught up', async () => {
      let consumerA = stream.createConsumer();
      let consumerB = stream.createConsumer();

      let consumeA = (async () => {
        for await (let packet of consumerA) {}
      })();
      let consumeB = (async () => {
        for await (let packet of consumerB) {}
      })();

      await wait(10);

      for (let i = 0; i < 10; i++) {
        stream.write('hello' + i);
      }

      await wait(50);

      assert.equal(stream.getQueueDepth(), 0);

      stream.close();
      await Promise.all([consumeA, consumeB]);
    });
  });
});
