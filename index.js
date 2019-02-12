const AsyncIterableStream = require('async-iterable-stream');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super();
    this._currentPacketId = 1;
    this._nextConsumerId = 1;
    this.maxBackpressure = 0;
    this._consumers = {};
    this._linkedListTailNode = {
      next: null,
      data: {
        value: undefined,
        done: false
      },
      packetId: this._currentPacketId
    };
  }

  _write(value, done) {
    this._currentPacketId++;
    let dataNode = {
      data: {value, done},
      next: null,
      packedId: this._currentPacketId
    };
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;
    this.maxBackpressure = 0;

    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;

    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      if (consumer.timeoutId !== undefined) {
        clearTimeout(consumer.timeoutId);
        delete consumer.timeoutId;
      }
      consumer.backpressure = this._currentPacketId - consumer.packetId;
      if (consumer.backpressure > this.maxBackpressure) {
        this.maxBackpressure = consumer.backpressure;
      }
      if (!consumer.isResolved) {
        consumer.isResolved = true;
        consumer.resolve();
      }
    }
  }

  write(value) {
    this._write(value, false);
  }

  close(value) {
    this._write(value, true);
  }

  getBackpressure(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.backpressure;
    }
    return null;
  }

  getBackpressureList() {
    let backpressureList = [];
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      backpressureList.push({
        consumerId: consumer.id,
        backpressure: this._consumers[consumer.id].backpressure
      });
    }
    return backpressureList;
  }

  async _waitForNextDataNode(consumerId, timeout) {
    return new Promise((resolve, reject) => {
      let timeoutId;
      if (timeout !== undefined) {
        // Create the error object in the outer scope in order
        // to get the full stack trace.
        let error = new Error('Stream consumer iteration timed out');
        (async () => {
          let delay = wait(timeout);
          timeoutId = delay.timeoutId;
          await delay.promise;
          error.name = 'TimeoutError';
          delete this._consumers[consumerId];
          reject(error);
        })();
      }
      this._consumers[consumerId] = {
        id: consumerId,
        resolve,
        timeoutId,
        packetId: this._currentPacketId
      };
    });
  }

  createAsyncIterator(timeout) {
    let consumerId = this._nextConsumerId++;
    let currentNode = this._linkedListTailNode;
    return {
      consumerId,
      next: async () => {
        if (!currentNode.next) {
          await this._waitForNextDataNode(consumerId, timeout);
        }
        currentNode = currentNode.next;
        let consumer = this._consumers[consumerId];
        if (consumer) {
          consumer.packetId = currentNode.packedId;
          consumer.backpressure = this._currentPacketId - consumer.packetId;
          if (currentNode.data.done) {
            delete this._consumers[consumerId];
          }
        }
        return currentNode.data;
      },
      return: () => {
        delete this._consumers[consumerId];
        return {};
      }
    };
  }
}

function wait(timeout) {
  let timeoutId;
  let promise = new Promise((resolve) => {
    timeoutId = setTimeout(resolve, timeout);
  });
  return {timeoutId, promise};
}

module.exports = WritableAsyncIterableStream;
