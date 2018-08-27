const BlockStream = require('block-stream2')
const stream = require('readable-stream')

class ChunkStoreWriteStream extends stream.Writable {
  constructor (store, chunkLength, opts = {}) {
    super(opts)

    if (!store || !store.put || !store.get) {
      throw new Error('First argument must be an abstract-chunk-store compliant store')
    }
    chunkLength = Number(chunkLength)
    if (!chunkLength) throw new Error('Second argument must be a chunk length')

    this._blockstream = new BlockStream(chunkLength, { zeroPadding: false })

    let index = 0
    const onData = chunk => {
      if (this.destroyed) return
      store.put(index, chunk)
      index += 1
    }

    this._blockstream
      .on('data', onData)
      .on('error', err => { this.destroy(err) })

    this.on('finish', () => this._blockstream.end())
  }

  _write (chunk, encoding, callback) {
    this._blockstream.write(chunk, encoding, callback)
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true

    if (err) this.emit('error', err)
    this.emit('close')
  }
}

module.exports = ChunkStoreWriteStream
