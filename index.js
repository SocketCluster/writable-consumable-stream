class IterableAsyncStream {
  constructor() {
    this._writeData = function () {};
    this._pendingPromise = null;
  }

  write(data) {
    this._writeData(data);
  }

  next() {
    return this.createDataStream().next();
  }

  async once() {
    return (await this.next()).value;
  }

  waitForNextDataBuffer() {
    if (this._pendingPromise) {
      return this._pendingPromise;
    }
    this._pendingPromise = new Promise((resolve) => {
      const dataBuffer = [];
      this._writeData = (data) => {
        dataBuffer.push(data);
        if (dataBuffer.length === 1) {
          resolve(dataBuffer);
        }
      };
    });
    return this._pendingPromise
    .then((buffer) => {
      delete this._pendingPromise;
      return buffer;
    });
  }

  async *createDataBufferStream() {
    while (true) {
      yield this.waitForNextDataBuffer();
    }
  }

  async *createDataStream() {
    const dataBufferStream = this.createDataBufferStream();
    for await (const dataBuffer of dataBufferStream) {
      for (const data of dataBuffer) {
        yield data;
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this.createDataStream();
  }
}

module.exports = IterableAsyncStream;
