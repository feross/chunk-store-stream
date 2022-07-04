const BlockStream = require('block-stream2')
const { Writable } = require('streamx')

class ChunkStoreWriteStream extends Writable {
  constructor (store, chunkLength, opts = {}) {
    super(opts)

    if (!store || !store.put || !store.get) {
      throw new Error('First argument must be an abstract-chunk-store compliant store')
    }
    chunkLength = Number(chunkLength)
    if (!chunkLength) throw new Error('Second argument must be a chunk length')

    const zeroPadding = opts.zeroPadding === undefined ? false : opts.zeroPadding
    this._blockstream = new BlockStream(chunkLength, { ...opts, zeroPadding })
    this._outstandingPuts = 0
    this._storeMaxOutstandingPuts = opts.storeMaxOutstandingPuts || 16

    let index = 0
    const onData = chunk => {
      if (this.destroyed) return

      this._outstandingPuts += 1
      if (this._outstandingPuts >= this._storeMaxOutstandingPuts) {
        this._blockstream.pause()
      }
      store.put(index, chunk, (err) => {
        if (err) return this.destroy(err)
        this._outstandingPuts -= 1
        if (this._outstandingPuts < this._storeMaxOutstandingPuts) {
          this._blockstream.resume()
        }
        if (this._outstandingPuts === 0 && typeof this._finalCb === 'function') {
          this._finalCb(null)
          this._finalCb = null
        }
      })
      index += 1
    }

    this._blockstream
      .on('data', onData)
      .on('error', err => { this.destroy(err) })
  }

  _write (chunk, callback) {
    this._blockstream.write(chunk, callback)
  }

  _final (cb) {
    this._blockstream.end()
    this._blockstream.once('end', () => {
      if (this._outstandingPuts === 0) cb(null)
      else this._finalCb = cb
    })
  }
}

module.exports = ChunkStoreWriteStream
