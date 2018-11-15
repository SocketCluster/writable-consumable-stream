const END_SYMBOL = Symbol('end');

class IterableAsyncStream {
  constructor() {
    this._writeData = () => {};
    this.waitForNextDataBuffer();
  }

  write(data) {
    this._writeData(data);
  }

  end() {
    this.write(END_SYMBOL);
  }

  next() {
    return this.createDataStream().next();
  }

  async once() {
    return (await this.next()).value;
  }

  async waitForNextDataBuffer() {
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
    const buffer = await this._pendingPromise;
    delete this._pendingPromise;
    return buffer;
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
        if (data === END_SYMBOL) {
          return;
        }
        yield data;
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this.createDataStream();
  }
}

module.exports = IterableAsyncStream;
