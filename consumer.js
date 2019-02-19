class Consumer {
  constructor(stream, id, startNode, timeout) {
    this.id = id;
    this._backpressure = 0;
    this.stream = stream;
    this.currentNode = startNode;
    this.timeout = timeout;
    this.stream.setConsumer(this.id, this);
  }

  getStats() {
    let stats = {
      id: this.id,
      backpressure: this._backpressure
    };
    if (this.timeout != null) {
      stats.timeout = this.timeout;
    }
    return stats;
  }

  setResolver(resolve) {
    this._resolve = resolve;
  }

  clearResolver() {
    delete this._resolve;
  }

  resetBackpressure() {
    this._backpressure = 0;
  }

  applyBackpressure(packet) {
    this._backpressure++;
  }

  releaseBackpressure(packet) {
    this._backpressure--;
  }

  getBackpressure() {
    return this._backpressure;
  }

  write(packet) {
    this.applyBackpressure(packet);
    if (this._resolve) {
      this._resolve();
      delete this._resolve;
    }
  }

  kill(value) {
    this._killPacket = {value, done: true};
    this.applyBackpressure(this._killPacket);
    if (this._resolve) {
      this._resolve();
      delete this._resolve;
    }
  }

  async next() {
    this.stream.setConsumer(this.id, this);
    while (true) {
      if (!this.currentNode.next) {
        try {
          await this.stream.waitForNextItem(this, this.timeout);
        } catch (error) {
          this.stream.removeConsumer(this.id);
          throw error;
        }
      }
      if (this._killPacket) {
        this.resetBackpressure();
        this.stream.removeConsumer(this.id);
        let killPacket = this._killPacket;
        delete this._killPacket;
        return killPacket;
      }

      this.currentNode = this.currentNode.next;
      this.releaseBackpressure(this.currentNode)

      if (this.currentNode.consumerId && this.currentNode.consumerId !== this.id) {
        continue;
      }

      if (this.currentNode.data.done) {
        this.stream.removeConsumer(this.id);
      }
      return this.currentNode.data;
    }
  }

  return() {
    this.stream.removeConsumer(this.id);
    return {};
  }
}

module.exports = Consumer;
