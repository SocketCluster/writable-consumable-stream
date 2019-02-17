class Consumer {
  constructor(stream, id, startNode, timeout) {
    this.id = id;
    this.backpressure = 0;
    this.stream = stream;
    this.currentNode = startNode;
    this.timeout = timeout;
    this.stream.setConsumer(this.id, this);
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
      if (this.kill) {
        this.backpressure = 0;
        this.stream.removeConsumer(this.id);
        let killPacket = this.kill;
        delete this.kill;
        return killPacket;
      }

      this.backpressure--;
      this.currentNode = this.currentNode.next;

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
