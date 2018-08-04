const stream = require('readable-stream')

class ChunkStoreReadStream extends stream.Readable {
  constructor (store, chunkLength, opts) {
    super(opts)

    if (!opts) opts = {}

    if (!store || !store.put || !store.get) {
      throw new Error('First argument must be an abstract-chunk-store compliant store')
    }
    chunkLength = Number(chunkLength)
    if (!chunkLength) throw new Error('Second argument must be a chunk length')

    this._length = opts.length || store.length
    if (!Number.isFinite(this._length)) throw new Error('missing required `length` property')

    this._store = store
    this._chunkLength = chunkLength
    this._index = 0
  }

  _read () {
    if (this._index * this._chunkLength >= this._length) {
      this.push(null)
    } else {
      this._store.get(this._index, (err, chunk) => {
        if (err) return this.destroy(err)
        this.push(chunk)
      })
    }
    this._index += 1
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true

    if (err) this.emit('error', err)
    this.emit('close')
  }
}

module.exports = ChunkStoreReadStream
