class LinkedList {
  constructor(value) {
    this.head = {next: null, sentinel: true};
    this.tail = this.head;
    this.length = 0;
  }

  append(value) {
    let node = {value, next: null};
    this.tail.next = node;
    this.tail = node;
    this.length++;
  }
}

module.exports = LinkedList;
