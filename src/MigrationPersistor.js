const Settings = require('./Settings')
const Logger = require('logger-sharelatex')
const Stream = require('stream')
const { callbackify, promisify } = require('util')
const { NotFoundError, WriteError } = require('./Errors')

const pipeline = promisify(Stream.pipeline)

// Persistor that wraps two other persistors. Talks to the 'primary' by default,
// but will fall back to an older persistor in the case of a not-found error.
// If `Settings.fallback.copyOnMiss` is set, this will copy files from the fallback
// to the primary, in the event that they are missing.
//
// It is unlikely that the bucket/location name will be the same on the fallback
// as the primary. The bucket names should be overridden in `Settings.fallback.buckets`
// e.g.
// Settings.fallback.buckets = {
//   myBucketOnS3: 'myBucketOnGCS'
// }

module.exports = function (primary, fallback) {
  function _wrapMethodOnBothPersistors(method) {
    return async function (bucket, key, ...moreArgs) {
      const fallbackBucket = _getFallbackBucket(bucket)

      await Promise.all([
        primary.promises[method](bucket, key, ...moreArgs),
        fallback.promises[method](fallbackBucket, key, ...moreArgs)
      ])
    }
  }

  async function getObjectStreamWithFallback(bucket, key, opts) {
    const shouldCopy = Settings.fallback.copyOnMiss && !opts.start && !opts.end

    try {
      return await primary.promises.getObjectStream(bucket, key, opts)
    } catch (err) {
      if (err instanceof NotFoundError) {
        const fallbackBucket = _getFallbackBucket(bucket)
        const fallbackStream = await fallback.promises.getObjectStream(
          fallbackBucket,
          key,
          opts
        )
        // tee the stream to the client, and as a copy to the primary (if necessary)
        // start listening on both straight away so that we don't consume bytes
        // in one place before the other
        const returnStream = new Stream.PassThrough()
        pipeline(fallbackStream, returnStream)

        if (shouldCopy) {
          const copyStream = new Stream.PassThrough()
          pipeline(fallbackStream, copyStream)

          _copyStreamFromFallbackAndVerify(
            copyStream,
            fallbackBucket,
            bucket,
            key,
            key
          ).catch(() => {
            // swallow errors, as this runs in the background and will log a warning
          })
        }
        return returnStream
      }
      throw err
    }
  }

  async function copyFileWithFallback(bucket, sourceKey, destKey) {
    try {
      return await primary.promises.copyObject(bucket, sourceKey, destKey)
    } catch (err) {
      if (err instanceof NotFoundError) {
        const fallbackBucket = _getFallbackBucket(bucket)
        const fallbackStream = await fallback.promises.getObjectStream(
          fallbackBucket,
          sourceKey,
          {}
        )

        const copyStream = new Stream.PassThrough()
        pipeline(fallbackStream, copyStream)

        if (Settings.fallback.copyOnMiss) {
          const missStream = new Stream.PassThrough()
          pipeline(fallbackStream, missStream)

          // copy from sourceKey -> sourceKey
          _copyStreamFromFallbackAndVerify(
            missStream,
            fallbackBucket,
            bucket,
            sourceKey,
            sourceKey
          ).then(() => {
            // swallow errors, as this runs in the background and will log a warning
          })
        }
        // copy from sourceKey -> destKey
        return _copyStreamFromFallbackAndVerify(
          copyStream,
          fallbackBucket,
          bucket,
          sourceKey,
          destKey
        )
      }
      throw err
    }
  }

  function _getFallbackBucket(bucket) {
    return Settings.fallback.buckets[bucket] || bucket
  }

  function _wrapFallbackMethod(method) {
    return async function (bucket, key, ...moreArgs) {
      try {
        return await primary.promises[method](bucket, key, ...moreArgs)
      } catch (err) {
        if (err instanceof NotFoundError) {
          const fallbackBucket = _getFallbackBucket(bucket)
          if (Settings.fallback.copyOnMiss) {
            const fallbackStream = await fallback.promises.getObjectStream(
              fallbackBucket,
              key,
              {}
            )
            // run in background
            _copyStreamFromFallbackAndVerify(
              fallbackStream,
              fallbackBucket,
              bucket,
              key,
              key
            ).catch((err) => {
              Logger.warn({ err }, 'failed to copy file from fallback')
            })
          }
          return fallback.promises[method](fallbackBucket, key, ...moreArgs)
        }
        throw err
      }
    }
  }

  async function _copyStreamFromFallbackAndVerify(
    stream,
    sourceBucket,
    destBucket,
    sourceKey,
    destKey
  ) {
    try {
      let sourceMd5
      try {
        sourceMd5 = await fallback.promises.getObjectMd5Hash(
          sourceBucket,
          sourceKey
        )
      } catch (err) {
        Logger.warn(err, 'error getting md5 hash from fallback persistor')
      }

      await primary.promises.sendStream(destBucket, destKey, stream, sourceMd5)
    } catch (err) {
      const error = new WriteError({
        message: 'unable to copy file to destination persistor',
        info: {
          sourceBucket,
          destBucket,
          sourceKey,
          destKey
        }
      }).withCause(err)
      if (Settings.Metrics) {
        Settings.Metrics.inc('fallback.copy.failure')
      }

      try {
        await primary.promises.deleteObject(destBucket, destKey)
      } catch (err) {
        error.info.cleanupError = new WriteError({
          message: 'unable to clean up destination copy artifact',
          info: {
            destBucket,
            destKey
          }
        }).withCause(err)
      }

      Logger.warn({ error }, 'failed to copy file from fallback')
      throw error
    }
  }

  return {
    primaryPersistor: primary,
    fallbackPersistor: fallback,
    sendFile: primary.sendFile,
    sendStream: primary.sendStream,
    getObjectStream: callbackify(getObjectStreamWithFallback),
    getRedirectUrl: primary.getRedirectUrl,
    getObjectMd5Hash: callbackify(_wrapFallbackMethod('getObjectMd5Hash')),
    deleteDirectory: callbackify(
      _wrapMethodOnBothPersistors('deleteDirectory')
    ),
    getObjectSize: callbackify(_wrapFallbackMethod('getObjectSize')),
    deleteObject: callbackify(_wrapMethodOnBothPersistors('deleteObject')),
    copyObject: callbackify(copyFileWithFallback),
    checkIfObjectExists: callbackify(
      _wrapFallbackMethod('checkIfObjectExists')
    ),
    directorySize: callbackify(_wrapFallbackMethod('directorySize')),
    promises: {
      sendFile: primary.promises.sendFile,
      sendStream: primary.promises.sendStream,
      getObjectStream: getObjectStreamWithFallback,
      getRedirectUrl: primary.promises.getRedirectUrl,
      getObjectMd5Hash: _wrapFallbackMethod('getObjectMd5Hash'),
      deleteDirectory: _wrapMethodOnBothPersistors('deleteDirectory'),
      getObjectSize: _wrapFallbackMethod('getObjectSize'),
      deleteObject: _wrapMethodOnBothPersistors('deleteObject'),
      copyObject: copyFileWithFallback,
      checkIfObjectExists: _wrapFallbackMethod('checkIfObjectExists'),
      directorySize: _wrapFallbackMethod('directorySize')
    }
  }
}
