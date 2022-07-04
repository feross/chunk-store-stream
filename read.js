const { Readable } = require('streamx')

class ChunkStoreReadStream extends Readable {
  constructor (store, chunkLength, opts = {}) {
    super(opts)

    if (!store || !store.put || !store.get) {
      throw new Error('First argument must be an abstract-chunk-store compliant store')
    }
    chunkLength = Number(chunkLength)
    if (!chunkLength) throw new Error('Second argument must be a chunk length')

    this._length = opts.length || store.length
    if (!Number.isFinite(this._length)) throw new Error('missing required `length` property')

    const offset = opts.offset || 0

    this._store = store
    this._chunkLength = chunkLength
    this._index = Math.floor(offset / chunkLength)
    this._chunkOffset = offset % chunkLength
    this._consumedLength = 0
  }

  _read (cb) {
    const remainingLength = this._length - this._consumedLength
    if (remainingLength <= 0) {
      this.push(null)
      cb()
    } else {
      const offset = this._chunkOffset
      const length = Math.min(remainingLength, this._chunkLength - this._chunkOffset)
      this._chunkOffset = 0
      this._consumedLength += length

      this._store.get(this._index, {
        offset,
        length
      }, (err, chunk) => {
        if (err) return this.destroy(err)
        this.push(chunk)
        cb()
      })
      this._index += 1
    }
  }
}

module.exports = ChunkStoreReadStream
