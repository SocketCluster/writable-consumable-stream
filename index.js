const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor(options) {
    options = options || {};
    super(() => {
      return this.createDataStream();
    });
    this.bufferTimeout = options.bufferTimeout || 10000;
    this._nextId = 1;
    this._dataConsumers = {};
  }

  write(data) { // TODO 2
    Object.keys(this._dataConsumers).forEach((consumerId) => {
      let consumer = this._dataConsumers[consumerId];
      if (Date.now() - consumer.time >= this.bufferTimeout) {
        delete this._dataConsumers[consumerId];
        return;
      }
      consumer.buffer.push(data);
      let callback = consumer.callback;
      if (callback) {
        delete consumer.callback;
        callback();
      }
    });
  }

  end() {
    this.write(END_SYMBOL);
  }

  async waitForNextDataBuffer(consumerId) {
    return new Promise((resolve) => {
      let buffer = [];
      this._dataConsumers[consumerId] = {
        time: Date.now(),
        buffer,
        callback: () => {
          resolve(buffer);
        }
      };
    });
  }

  async *createDataBufferStream(consumerId) {
    while (true) {
      yield this.waitForNextDataBuffer(consumerId);
    }
  }

  async *createDataStream() {
    let consumerId = this._nextId++;
    let dataBufferStream = this.createDataBufferStream(consumerId);
    for await (let dataBuffer of dataBufferStream) {
      for (let data of dataBuffer) {
        if (data === END_SYMBOL) {
          delete this._dataConsumers[consumerId];
          return;
        }
        yield data;
      }
    }
  }
}

module.exports = WritableAsyncIterableStream;
