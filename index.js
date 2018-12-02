const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super(() => {
      return this.createDataStream();
    });
    this._nextConsumerId = 1;
    this._consumers = {};
    this._linkedListTailNode = {value: undefined, next: null, sentinel: true};
  }

  write(data) {
    let dataNode = {value: data, next: null};
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;
    Object.keys(this._consumers).forEach((consumerId) => {
      let consumer = this._consumers[consumerId];
      delete this._consumers[consumerId];
      consumer.callback();
    });
  }

  end() {
    this.write(END_SYMBOL);
  }

  async _waitForNextDataBuffer(consumerId) {
    return new Promise((resolve) => {
      let currentConsumer = this._consumers[consumerId];
      let startNode = this._linkedListTailNode;
      this._consumers[consumerId] = {
        startNode,
        callback: () => {
          resolve(startNode);
        }
      };
    });
  }

  async *createDataBufferStream() {
    let consumerId = this._nextConsumerId++;
    while (true) {
      yield this._waitForNextDataBuffer(consumerId);
    }
  }

  async *createDataStream() {
    let dataBufferStream = this.createDataBufferStream();
    for await (let startNode of dataBufferStream) {
      let currentNode = startNode.next;
      while (currentNode) {
        let data = currentNode.value;
        if (data === END_SYMBOL) {
          return;
        }
        yield data;
        currentNode = currentNode.next;
      }
    }
  }
}

module.exports = WritableAsyncIterableStream;
